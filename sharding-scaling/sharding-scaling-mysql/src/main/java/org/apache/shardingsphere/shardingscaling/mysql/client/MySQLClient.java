/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.shardingscaling.mysql.client;

import org.apache.shardingsphere.database.protocol.codec.PacketCodec;
import org.apache.shardingsphere.database.protocol.mysql.codec.MySQLPacketCodecEngine;
import org.apache.shardingsphere.database.protocol.mysql.packet.command.binlog.MySQLComBinlogDumpCommandPacket;
import org.apache.shardingsphere.database.protocol.mysql.packet.command.binlog.MySQLComRegisterSlaveCommandPacket;
import org.apache.shardingsphere.database.protocol.mysql.packet.command.query.text.query.MySQLComQueryPacket;
import org.apache.shardingsphere.database.protocol.mysql.packet.generic.MySQLErrPacket;
import org.apache.shardingsphere.database.protocol.mysql.packet.generic.MySQLOKPacket;
import org.apache.shardingsphere.shardingscaling.mysql.client.netty.MySQLNegotiateHandler;
import org.apache.shardingsphere.shardingscaling.mysql.client.netty.MySQLBinlogEventPacketDecoder;
import org.apache.shardingsphere.shardingscaling.mysql.client.netty.MySQLCommandPacketDecoder;
import org.apache.shardingsphere.shardingscaling.mysql.binlog.event.AbstractBinlogEvent;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Promise;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;

/**
 * MySQL Connector.
 */
@RequiredArgsConstructor
@Slf4j
public final class MySQLClient {
    
    private final int serverId;
    
    private final String host;
    
    private final int port;
    
    private final String username;
    
    private final String password;
    
    private EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);
    
    private Channel channel;
    
    private Promise<Object> responseCallback;
    
    private ArrayBlockingQueue<AbstractBinlogEvent> blockingEventQueue = new ArrayBlockingQueue(1000);
    
    private ServerInfo serverInfo;
    
    /**
     * Connect to MySQL.
     */
    public synchronized void connect() {
        responseCallback = new DefaultPromise<>(eventLoopGroup.next());
        channel = new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(final SocketChannel socketChannel) {
                        socketChannel.pipeline().addLast(new PacketCodec(new MySQLPacketCodecEngine()));
                        socketChannel.pipeline().addLast(new MySQLCommandPacketDecoder());
                        socketChannel.pipeline().addLast(new MySQLNegotiateHandler(username, password, responseCallback));
                        socketChannel.pipeline().addLast(new MySQLCommandResponseHandler());
                    }
                })
                .option(ChannelOption.AUTO_READ, true)
                .connect(host, port).channel();
        serverInfo = waitExpectedResponse(ServerInfo.class);
    }
    
    /**
     * Execute command.
     *
     * @param queryString query string
     * @return true if execute successfully, otherwise false
     */
    public synchronized boolean execute(final String queryString) {
        responseCallback = new DefaultPromise<>(eventLoopGroup.next());
        MySQLComQueryPacket comQueryPacket = new MySQLComQueryPacket(queryString);
        channel.writeAndFlush(comQueryPacket);
        return null != waitExpectedResponse(MySQLOKPacket.class);
    }
    
    /**
     * Execute update.
     *
     * @param queryString query string
     * @return affected rows
     */
    public synchronized int executeUpdate(final String queryString) {
        responseCallback = new DefaultPromise<>(eventLoopGroup.next());
        MySQLComQueryPacket comQueryPacket = new MySQLComQueryPacket(queryString);
        channel.writeAndFlush(comQueryPacket);
        return (int) waitExpectedResponse(MySQLOKPacket.class).getAffectedRows();
    }
    
    /**
     * Execute query.
     *
     * @param queryString query string
     * @return result set
     */
    public synchronized InternalResultSet executeQuery(final String queryString) {
        responseCallback = new DefaultPromise<>(eventLoopGroup.next());
        MySQLComQueryPacket comQueryPacket = new MySQLComQueryPacket(queryString);
        channel.writeAndFlush(comQueryPacket);
        return waitExpectedResponse(InternalResultSet.class);
    }
    
    /**
     * Start dump binlog.
     *
     * @param binlogFileName binlog file name
     * @param binlogPosition binlog position
     */
    public synchronized void subscribe(final String binlogFileName, final long binlogPosition) {
        initDumpConnectSession();
        registerSlave();
        dumpBinlog(binlogFileName, binlogPosition, queryChecksumLength());
    }
    
    private void initDumpConnectSession() {
        if (serverInfo.getServerVersion().greaterThanOrEqualTo(5, 6, 0)) {
            execute("SET @MASTER_BINLOG_CHECKSUM= @@GLOBAL.BINLOG_CHECKSUM");
        }
    }
    
    private void registerSlave() {
        responseCallback = new DefaultPromise<>(eventLoopGroup.next());
        InetSocketAddress localAddress = (InetSocketAddress) channel.localAddress();
        MySQLComRegisterSlaveCommandPacket registerSlaveCommandPacket = new MySQLComRegisterSlaveCommandPacket(serverId, localAddress.getHostName(), username, password, localAddress.getPort());
        channel.writeAndFlush(registerSlaveCommandPacket);
        waitExpectedResponse(MySQLOKPacket.class);
    }
    
    private int queryChecksumLength() {
        if (!serverInfo.getServerVersion().greaterThanOrEqualTo(5, 6, 0)) {
            return 0;
        }
        InternalResultSet resultSet = executeQuery("SELECT @@GLOBAL.BINLOG_CHECKSUM");
        String checksumType = resultSet.getFieldValues().get(0).getData().get(0).toString();
        switch (checksumType) {
            case "None":
                return 0;
            case "CRC32":
                return 4;
            default:
                throw new UnsupportedOperationException();
        }
    }
    
    private void dumpBinlog(final String binlogFileName, final long binlogPosition, final int checksumLength) {
        responseCallback = null;
        channel.pipeline().remove(MySQLCommandPacketDecoder.class);
        channel.pipeline().remove(MySQLCommandResponseHandler.class);
        channel.pipeline().addLast(new MySQLBinlogEventPacketDecoder(checksumLength));
        channel.pipeline().addLast(new MySQLBinlogEventHandler());
        channel.writeAndFlush(new MySQLComBinlogDumpCommandPacket((int) binlogPosition, serverId, binlogFileName));
    }

    /**
     * Poll binlog event.
     *
     * @return binlog event
     */
    public synchronized AbstractBinlogEvent poll() {
        return blockingEventQueue.poll();
    }
    
    private <T> T waitExpectedResponse(final Class<T> type) {
        try {
            Object response = responseCallback.get();
            if (null == response) {
                return null;
            }
            if (type.equals(response.getClass())) {
                return (T) response;
            }
            if (response instanceof MySQLErrPacket) {
                throw new RuntimeException(((MySQLErrPacket) response).getErrorMessage());
            }
            throw new RuntimeException("unexpected response type");
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
    
    class MySQLCommandResponseHandler extends ChannelInboundHandlerAdapter {
        
        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            if (null != responseCallback) {
                responseCallback.setSuccess(msg);
            }
        }
        
        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            if (null != responseCallback) {
                responseCallback.setFailure(cause);
                log.error("protocol resolution error", cause);
            }
        }
    }
    
    class MySQLBinlogEventHandler extends ChannelInboundHandlerAdapter {
        
        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
            if (msg instanceof AbstractBinlogEvent) {
                blockingEventQueue.put((AbstractBinlogEvent) msg);
            }
        }
        
        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
            log.error("protocol resolution error", cause);
        }
    }
}
