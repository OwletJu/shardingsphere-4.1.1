package com.roy.demo;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.shardingsphere.api.config.sharding.KeyGeneratorConfiguration;
import org.apache.shardingsphere.api.config.sharding.ShardingRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.TableRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.strategy.InlineShardingStrategyConfiguration;
import org.apache.shardingsphere.api.config.sharding.strategy.StandardShardingStrategyConfiguration;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.shardingjdbc.api.ShardingDataSourceFactory;
import org.apache.shardingsphere.underlying.common.config.inline.InlineExpressionParser;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author ：楼兰
 * @date ：Created in 2020/12/16
 * @description: 复制application02.properties中的分库分表逻辑
 **/

public class ShardingJDBCDemo {

    public static void main(String[] args) throws SQLException {

        //=======一、配置数据库
        Map<String, DataSource> dataSourceMap = new HashMap<>(2);//为两个数据库的datasource
        // 配置第一个数据源
        HikariDataSource dataSource0 = new HikariDataSource();
        dataSource0.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource0.setJdbcUrl("jdbc:mysql://localhost:3306/coursedb?serverTimezone=GMT%2B8&useSSL=false");
        dataSource0.setUsername("root");
        dataSource0.setPassword("root");
        dataSourceMap.put("m1", dataSource0);
        // 配置第二个数据源
        HikariDataSource dataSource1 = new HikariDataSource();
        dataSource1.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource1.setJdbcUrl("jdbc:mysql://localhost:3306/coursedb2?serverTimezone=GMT%2B8&useSSL=false");
        dataSource1.setUsername("root");
        dataSource1.setPassword("root");
        dataSourceMap.put("m2", dataSource1);

        //=======二、配置分库分表策略
        // 配置分片规则
        ShardingRuleConfiguration shardingRuleConfig = new ShardingRuleConfiguration();
        //真实表分布
        TableRuleConfiguration courseTableRuleConfig = new TableRuleConfiguration("course",
                "m$->{1..2}.course_$->{1..2}");
        //主键策略
        courseTableRuleConfig.setKeyGeneratorConfig(new KeyGeneratorConfiguration("SNOWFLAKE"
                , "cid", getProps()));
        //真实库分布
        courseTableRuleConfig.setDatabaseShardingStrategyConfig(new InlineShardingStrategyConfiguration("cid",
                "m$->{cid%2+1}"));

        courseTableRuleConfig.setTableShardingStrategyConfig(new InlineShardingStrategyConfiguration("cid",
                "course_$->{cid%2+1}"));
        shardingRuleConfig.getTableRuleConfigs().add(courseTableRuleConfig);
        //绑定表配置
//        shardingRuleConfig.getBindingTableGroups().add("t_order, t_order_item");
        //配置默认的分库策略 可选
        shardingRuleConfig.setDefaultDatabaseShardingStrategyConfig(new InlineShardingStrategyConfiguration("cid", "m$->{cid%2+1}"));
        //配置默认的分表策略 可选
        shardingRuleConfig.setDefaultTableShardingStrategyConfig(new StandardShardingStrategyConfiguration("cid", new PreciseShardingAlgorithm<Long>() {
            @Override
            public String doSharding(Collection<String> collection, final PreciseShardingValue<Long> preciseShardingValue) {
                for (String each : collection) {
                    if (each.endsWith(preciseShardingValue.getValue() % 2 + "")) {//这句话会产生什么？只会产生偶数的订单
                        return each;
                    }
                }
                throw new UnsupportedOperationException();
            }
        }));

        //三、配置属性值
        Properties properties = new Properties();
        //打开日志输出
        properties.setProperty("sql.show", "true");
        //K1 创建ShardingSphere的数据源 ShardingDataSource
        DataSource dataSource = ShardingDataSourceFactory.createDataSource(dataSourceMap, shardingRuleConfig, properties);

        //-------------测试部分-----------------//
        ShardingJDBCDemo test = new ShardingJDBCDemo();
        //建表
//        test.droptable(dataSource);
//        test.createtable(dataSource);

        //插入数据
//        test.addcourse(dataSource);
        //K1 调试的起点 查询数据
        test.querycourse(dataSource);
    }

    //雪花算法，需要有机器序号，手动配置序号
    private static Properties getProps() {
        Properties props = new Properties();
        props.setProperty("worker.id", "123");
        return props;
    }

    //添加10条课程记录
    public void addcourse(DataSource dataSource) throws SQLException {
        for (int i = 1; i < 10; i++) {
            long orderId = executeAndGetGeneratedKey(dataSource, "INSERT INTO course (cname, user_id, cstatus) VALUES ('java'," + i + ", '1')");
            System.out.println("添加课程成功，课程ID：" + orderId);
        }
    }

    public void querycourse(DataSource dataSource) throws SQLException {
        Connection conn = null;
        try {
            //ShardingConnectioin
            conn = dataSource.getConnection();
            //ShardingStatement
            Statement statement = conn.createStatement();
            String sql = "SELECT cid,cname,user_id,cstatus from course";
            //ShardingResultSet
            ResultSet result = statement.executeQuery(sql);
            while (result.next()) {
                System.out.println("result:" + result.getInt("cid"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (null != conn) {
                conn.close();
            }
        }
    }

    private void execute(final DataSource dataSource, final String sql) throws SQLException {
        try (
                Connection conn = dataSource.getConnection();
                Statement statement = conn.createStatement()) {
            statement.execute(sql);
        }
    }

    private long executeAndGetGeneratedKey(final DataSource dataSource, final String sql) throws SQLException {
        long result = -1;
        try (
                Connection conn = dataSource.getConnection();
                Statement statement = conn.createStatement()) {
            statement.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
            ResultSet resultSet = statement.getGeneratedKeys();
            if (resultSet.next()) {
                result = resultSet.getLong(1);
            }
        }
        return result;
    }

    /**
     * -----------------------------表初始化--------------------------------
     */
    public void droptable(DataSource dataSource) throws SQLException {
        execute(dataSource, "DROP TABLE IF EXISTS course_1");
        execute(dataSource, "DROP TABLE IF EXISTS course_2");
    }

    public void createtable(DataSource dataSource) throws SQLException {
        execute(dataSource, "CREATE TABLE course_1 (cid BIGINT(20) PRIMARY KEY,cname VARCHAR(50) NOT NULL,user_id BIGINT(20) NOT NULL,cstatus varchar(10) NOT NULL);");
        execute(dataSource, "CREATE TABLE course_2 (cid BIGINT(20) PRIMARY KEY,cname VARCHAR(50) NOT NULL,user_id BIGINT(20) NOT NULL,cstatus varchar(10) NOT NULL);");
    }
}
