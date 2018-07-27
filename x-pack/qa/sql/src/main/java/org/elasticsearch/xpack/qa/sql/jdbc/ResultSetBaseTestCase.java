/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.qa.sql.jdbc;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcConfiguration;
import org.elasticsearch.xpack.sql.jdbc.jdbcx.JdbcDataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract class ResultSetBaseTestCase extends JdbcIntegrationTestCase {
    
    static final Set<String> fieldsNames = Stream.of("test_byte", "test_integer", "test_long", "test_short", "test_double",
            "test_float", "test_keyword")
            .collect(Collectors.toCollection(HashSet::new));

    protected void createIndex(String index) throws Exception {
        Request request = new Request("PUT", "/" + index);
        XContentBuilder createIndex = JsonXContent.contentBuilder().startObject();
        createIndex.startObject("settings");
        {
            createIndex.field("number_of_shards", 1);
            createIndex.field("number_of_replicas", 1);
        }
        createIndex.endObject();
        createIndex.startObject("mappings");
        {
            createIndex.startObject("doc");
            {
                createIndex.startObject("properties");
                {}
                createIndex.endObject();
            }
            createIndex.endObject();
        }
        createIndex.endObject().endObject();
        request.setJsonEntity(Strings.toString(createIndex));
        client().performRequest(request);
    }

    protected void updateMapping(String index, CheckedConsumer<XContentBuilder, IOException> body) throws Exception {
        Request request = new Request("PUT", "/" + index + "/_mapping/doc");
        XContentBuilder updateMapping = JsonXContent.contentBuilder().startObject();
        updateMapping.startObject("properties");
        {
            body.accept(updateMapping);
        }
        updateMapping.endObject().endObject();
        
        request.setJsonEntity(Strings.toString(updateMapping));
        client().performRequest(request);
    }

    protected void createTestDataForByteValueTests(byte random1, byte random2, byte random3) throws Exception, IOException {
        createIndex("test");
        updateMapping("test", builder -> {
            builder.startObject("test_byte").field("type", "byte").endObject();
            builder.startObject("test_null_byte").field("type", "byte").endObject();
            builder.startObject("test_keyword").field("type", "keyword").endObject();
        });
        
        index("test", "1", builder -> {
            builder.field("test_byte", random1);
            builder.field("test_null_byte", (Byte) null);
        });
        index("test", "2", builder -> {
            builder.field("test_byte", random2);
            builder.field("test_keyword", random3);
        });
    }

    protected void createTestDataForShortValueTests(short random1, short random2, short random3) throws Exception, IOException {
        createIndex("test");
        updateMapping("test", builder -> {
            builder.startObject("test_short").field("type", "short").endObject();
            builder.startObject("test_null_short").field("type", "short").endObject();
            builder.startObject("test_keyword").field("type", "keyword").endObject();
        });
        
        index("test", "1", builder -> {
            builder.field("test_short", random1);
            builder.field("test_null_short", (Short) null);
        });
        index("test", "2", builder -> {
            builder.field("test_short", random2);
            builder.field("test_keyword", random3);
        });
    }

    protected void createTestDataForIntegerValueTests(int random1, int random2, int random3) throws Exception, IOException {
        createIndex("test");
        updateMapping("test", builder -> {
            builder.startObject("test_integer").field("type", "integer").endObject();
            builder.startObject("test_null_integer").field("type", "integer").endObject();
            builder.startObject("test_keyword").field("type", "keyword").endObject();
        });
        
        index("test", "1", builder -> {
            builder.field("test_integer", random1);
            builder.field("test_null_integer", (Integer) null);
        });
        index("test", "2", builder -> {
            builder.field("test_integer", random2);
            builder.field("test_keyword", random3);
        });
    }

    protected void createTestDataForLongValueTests(long random1, long random2, long random3) throws Exception, IOException {
        createIndex("test");
        updateMapping("test", builder -> {
            builder.startObject("test_long").field("type", "long").endObject();
            builder.startObject("test_null_long").field("type", "long").endObject();
            builder.startObject("test_keyword").field("type", "keyword").endObject();
        });
        
        index("test", "1", builder -> {
            builder.field("test_long", random1);
            builder.field("test_null_long", (Long) null);
        });
        index("test", "2", builder -> {
            builder.field("test_long", random2);
            builder.field("test_keyword", random3);
        });
    }

    protected void createTestDataForDoubleValueTests(double random1, double random2, double random3) throws Exception, IOException {
        createIndex("test");
        updateMapping("test", builder -> {
            builder.startObject("test_double").field("type", "double").endObject();
            builder.startObject("test_null_double").field("type", "double").endObject();
            builder.startObject("test_keyword").field("type", "keyword").endObject();
        });
        
        index("test", "1", builder -> {
            builder.field("test_double", random1);
            builder.field("test_null_double", (Double) null);
        });
        index("test", "2", builder -> {
            builder.field("test_double", random2);
            builder.field("test_keyword", random3);
        });
    }

    protected void createTestDataForFloatValueTests(float random1, float random2, float random3) throws Exception, IOException {
        createIndex("test");
        updateMapping("test", builder -> {
            builder.startObject("test_float").field("type", "float").endObject();
            builder.startObject("test_null_float").field("type", "float").endObject();
            builder.startObject("test_keyword").field("type", "keyword").endObject();
        });
        
        index("test", "1", builder -> {
            builder.field("test_float", random1);
            builder.field("test_null_float", (Double) null);
        });
        index("test", "2", builder -> {
            builder.field("test_float", random2);
            builder.field("test_keyword", random3);
        });
    }

    /**
     * Creates test data for all numeric get* methods. All values random and different from the other numeric fields already generated.
     * It returns a map containing the field name and its randomly generated value to be later used in checking the returned values.
     */
    protected Map<String,Number> createTestDataForNumericValueTypes(Supplier<Number> randomGenerator) throws Exception, IOException {
        Map<String,Number> map = new HashMap<String,Number>();
        createIndex("test");
        updateMappingForNumericValuesTests("test");
    
        index("test", "1", builder -> {
            // random Byte
            byte test_byte = randomValueOtherThanMany(map::containsValue, randomGenerator).byteValue();
            builder.field("test_byte", test_byte);
            map.put("test_byte", test_byte);
            
            // random Integer
            int test_integer = randomValueOtherThanMany(map::containsValue, randomGenerator).intValue();
            builder.field("test_integer", test_integer);
            map.put("test_integer", test_integer);
    
            // random Short
            int test_short = randomValueOtherThanMany(map::containsValue, randomGenerator).shortValue();
            builder.field("test_short", test_short);
            map.put("test_short", test_short);
            
            // random Long
            long test_long = randomValueOtherThanMany(map::containsValue, randomGenerator).longValue();
            builder.field("test_long", test_long);
            map.put("test_long", test_long);
            
            // random Double
            double test_double = randomValueOtherThanMany(map::containsValue, randomGenerator).doubleValue();
            builder.field("test_double", test_double);
            map.put("test_double", test_double);
            
            // random Float
            float test_float = randomValueOtherThanMany(map::containsValue, randomGenerator).floatValue();
            builder.field("test_float", test_float);
            map.put("test_float", test_float);
        });
        return map;
    }

    protected void updateMappingForNumericValuesTests(String indexName) throws Exception {
        updateMapping(indexName, builder -> {
            for(String field : fieldsNames) {
                builder.startObject(field).field("type", field.substring(5)).endObject();
            }
        });
    }

    protected long randomMillisSinceEpoch() {
        // random between Jan 1st, 1970 and Jan 1st, 2050
        return randomLongBetween(0, 2524608000000L);
    }

    protected float randomFloatBetween(float start, float end) {
        float result = 0.0f;
        while (result < start || result > end || Float.isNaN(result)) {
            result = start + randomFloat() * (end - start);
        }
        
        return result;
    }

    protected Long getMaxIntPlusOne() {
        return Long.valueOf(Integer.MAX_VALUE) + 1L;
    }

    protected Double getMaxLongPlusOne() {
        return Double.valueOf(Long.MAX_VALUE) + 1d;
    }

    protected Connection esJdbc(String timeZoneId) throws SQLException {
        return randomBoolean() ? useDriverManager(timeZoneId) : useDataSource(timeZoneId);
    }

    private Connection useDriverManager(String timeZoneId) throws SQLException {
        String elasticsearchAddress = getProtocol() + "://" + elasticsearchAddress();
        String address = "jdbc:es://" + elasticsearchAddress;
        Properties connectionProperties = connectionProperties();
        connectionProperties.put(JdbcConfiguration.TIME_ZONE, timeZoneId);
        Connection connection = DriverManager.getConnection(address, connectionProperties);
        
        assertNotNull("The timezone should be specified", connectionProperties.getProperty(JdbcConfiguration.TIME_ZONE));
        return connection;
    }

    private Connection useDataSource(String timeZoneId) throws SQLException {
        String elasticsearchAddress = getProtocol() + "://" + elasticsearchAddress();
        JdbcDataSource dataSource = new JdbcDataSource();
        String address = "jdbc:es://" + elasticsearchAddress;
        dataSource.setUrl(address);
        Properties connectionProperties = connectionProperties();
        connectionProperties.put(JdbcConfiguration.TIME_ZONE, timeZoneId);
        dataSource.setProperties(connectionProperties);
        Connection connection = dataSource.getConnection();
        
        assertNotNull("The timezone should be specified", connectionProperties.getProperty(JdbcConfiguration.TIME_ZONE));
        return connection;
    }

}