/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.sql.jdbc.EsType;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Calendar.DAY_OF_MONTH;
import static java.util.Calendar.ERA;
import static java.util.Calendar.HOUR_OF_DAY;
import static java.util.Calendar.MILLISECOND;
import static java.util.Calendar.MINUTE;
import static java.util.Calendar.MONTH;
import static java.util.Calendar.SECOND;
import static java.util.Calendar.YEAR;
import static org.elasticsearch.xpack.sql.qa.jdbc.JdbcTestUtils.JDBC_TIMEZONE;
import static org.elasticsearch.xpack.sql.qa.jdbc.JdbcTestUtils.asDate;
import static org.elasticsearch.xpack.sql.qa.jdbc.JdbcTestUtils.asTime;
import static org.elasticsearch.xpack.sql.qa.jdbc.JdbcTestUtils.of;

public class ResultSetTestCase extends JdbcIntegrationTestCase {
    
    static final Set<String> fieldsNames = Stream.of("test_byte", "test_integer", "test_long", "test_short", "test_double",
            "test_float", "test_keyword")
            .collect(Collectors.toCollection(HashSet::new));
    static final Map<Tuple<String, Object>, SQLType> dateTimeTestingFields = new HashMap<>();
    static final String SELECT_ALL_FIELDS = "SELECT test_boolean, test_byte, test_integer,"
            + "test_long, test_short, test_double, test_float, test_keyword, test_date FROM test";
    static final String SELECT_WILDCARD = "SELECT * FROM test";
    static {
        dateTimeTestingFields.put(new Tuple<String, Object>("test_boolean", true), EsType.BOOLEAN);
        dateTimeTestingFields.put(new Tuple<String, Object>("test_byte", 1), EsType.BYTE);
        dateTimeTestingFields.put(new Tuple<String, Object>("test_integer", 1), EsType.INTEGER);
        dateTimeTestingFields.put(new Tuple<String, Object>("test_long", 1L), EsType.LONG);
        dateTimeTestingFields.put(new Tuple<String, Object>("test_short", 1), EsType.SHORT);
        dateTimeTestingFields.put(new Tuple<String, Object>("test_double", 1d), EsType.DOUBLE);
        dateTimeTestingFields.put(new Tuple<String, Object>("test_float", 1f), EsType.FLOAT);
        dateTimeTestingFields.put(new Tuple<String, Object>("test_keyword", "true"), EsType.KEYWORD);
    }

    private String timeZoneId;

    @Before
    public void chooseRandomTimeZone() {
        this.timeZoneId = randomKnownTimeZone();
    }

    public void testMultiValueFieldWithMultiValueLeniencyEnabled() throws Exception {
        createTestDataForMultiValueTests();

        doWithQuery(() -> esWithLeniency(true), "SELECT int, keyword FROM test", (results) -> {
            results.next();
            Object number = results.getObject(1);
            Object string = results.getObject(2);
            assertEquals(-10, number);
            assertEquals("-10", string);
            assertFalse(results.next());
        });
    }

    public void testMultiValueFieldWithMultiValueLeniencyDisabled() throws Exception {
        createTestDataForMultiValueTests();

        SQLException expected = expectThrows(SQLException.class,
                () -> doWithQuery(() -> esWithLeniency(false), "SELECT int, keyword FROM test", (results) -> {
        }));
        assertTrue(expected.getMessage().contains("Arrays (returned by [int]) are not supported"));
        
        // default has multi value disabled
        expected = expectThrows(SQLException.class,
                () -> doWithQuery(() -> esJdbc(), "SELECT int, keyword FROM test", (results) -> {
        }));
    }
    
    public void testMultiValueFields_InsideObjects_WithMultiValueLeniencyEnabled() throws Exception {
        createTestDataForMultiValuesInObjectsTests();

        doWithQuery(() -> esWithLeniency(true), "SELECT object.intsubfield, object.textsubfield, object.textsubfield.keyword FROM test",
                (results) -> {
                    results.next();
                    Object number = results.getObject(1);
                    Object text = results.getObject(2);
                    Object keyword = results.getObject(3);
                    assertEquals(-25, number);
                    assertEquals("-25", text);
                    assertEquals("-25", keyword);
                    assertFalse(results.next());
        });
    }
    
    public void testMultiValueFields_InsideObjects_WithMultiValueLeniencyDisabled() throws Exception {
        createTestDataForMultiValuesInObjectsTests();

        SQLException expected = expectThrows(SQLException.class,
                () -> doWithQuery(() -> esWithLeniency(false), "SELECT object.intsubfield, object.textsubfield, object.textsubfield.keyword"
                        + " FROM test", (results) -> {
        }));
        assertTrue(expected.getMessage().contains("Arrays (returned by [object.intsubfield]) are not supported"));
        
        // default has multi value disabled
        expected = expectThrows(SQLException.class,
                () -> doWithQuery(() -> esJdbc(), "SELECT object.intsubfield, object.textsubfield, object.textsubfield.keyword",
                        (results) -> {
        }));
    }

    // Byte values testing
    public void testGettingValidByteWithoutCasting() throws Exception {
        byte random1 = randomByte();
        byte random2 = randomValueOtherThan(random1, () -> randomByte());
        byte random3 = randomValueOtherThanMany(Arrays.asList(random1, random2)::contains, () -> randomByte());
        
        createTestDataForByteValueTests(random1, random2, random3);
        
        doWithQuery("SELECT test_byte, test_null_byte, test_keyword FROM test", (results) -> {
            ResultSetMetaData resultSetMetaData = results.getMetaData();

            results.next();
            assertEquals(3, resultSetMetaData.getColumnCount());
            assertEquals(Types.TINYINT, resultSetMetaData.getColumnType(1));
            assertEquals(Types.TINYINT, resultSetMetaData.getColumnType(2));
            assertEquals(random1, results.getByte(1));
            assertEquals(random1, results.getByte("test_byte"));
            assertEquals(random1, (byte) results.getObject("test_byte", Byte.class));
            assertTrue(results.getObject(1) instanceof Byte);
            
            assertEquals(0, results.getByte(2));
            assertTrue(results.wasNull());
            assertEquals(null, results.getObject("test_null_byte"));
            assertTrue(results.wasNull());
            
            assertTrue(results.next());
            assertEquals(random2, results.getByte(1));
            assertEquals(random2, results.getByte("test_byte"));
            assertTrue(results.getObject(1) instanceof Byte);
            assertEquals(random3, results.getByte("test_keyword"));
            
            assertFalse(results.next());
        });
    }
    
    public void testGettingValidByteWithCasting() throws Exception {
        Map<String,Number> map = createTestDataForNumericValueTypes(() -> randomByte());
        
        doWithQuery(SELECT_WILDCARD, (results) -> {
            results.next();
            for(Entry<String, Number> e : map.entrySet()) {
                byte actual = results.getObject(e.getKey(), Byte.class);
                if (e.getValue() instanceof Double) {
                    assertEquals("For field " + e.getKey(), Math.round(e.getValue().doubleValue()), results.getByte(e.getKey()));
                    assertEquals("For field " + e.getKey(), Math.round(e.getValue().doubleValue()), actual);
                } else if (e.getValue() instanceof Float) {
                    assertEquals("For field " + e.getKey(), Math.round(e.getValue().floatValue()), results.getByte(e.getKey()));
                    assertEquals("For field " + e.getKey(), Math.round(e.getValue().floatValue()), actual);
                } else {
                    assertEquals("For field " + e.getKey(), e.getValue().byteValue(), results.getByte(e.getKey()));
                    assertEquals("For field " + e.getKey(), e.getValue().byteValue(), actual);
                }
            }
        });
    }

    public void testGettingInvalidByte() throws Exception {
        createIndex("test");
        updateMappingForNumericValuesTests("test");
        updateMapping("test", builder -> {
            builder.startObject("test_keyword").field("type", "keyword").endObject();
            builder.startObject("test_date").field("type", "date").endObject();
        });
        
        int intNotByte = randomIntBetween(Byte.MAX_VALUE + 1, Integer.MAX_VALUE);
        long longNotByte = randomLongBetween(Byte.MAX_VALUE + 1, Long.MAX_VALUE);
        short shortNotByte = (short) randomIntBetween(Byte.MAX_VALUE + 1, Short.MAX_VALUE);
        double doubleNotByte = randomDoubleBetween(Byte.MAX_VALUE + 1, Double.MAX_VALUE, true);
        float floatNotByte = randomFloatBetween(Byte.MAX_VALUE + 1, Float.MAX_VALUE);
        String randomString = randomUnicodeOfCodepointLengthBetween(128, 256);
        long randomDate = randomNonNegativeLong();
        
        String doubleErrorMessage = (doubleNotByte > Long.MAX_VALUE || doubleNotByte < Long.MIN_VALUE) ?
                Double.toString(doubleNotByte) : Long.toString(Math.round(doubleNotByte));
                
        index("test", "1", builder -> {
            builder.field("test_integer", intNotByte);
            builder.field("test_long", longNotByte);
            builder.field("test_short", shortNotByte);
            builder.field("test_double", doubleNotByte);
            builder.field("test_float", floatNotByte);
            builder.field("test_keyword", randomString);
            builder.field("test_date", randomDate);
        });
        
        doWithQuery(SELECT_WILDCARD, (results) -> {
            results.next();
            
            SQLException sqle = expectThrows(SQLException.class, () -> results.getByte("test_integer"));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", intNotByte), sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_integer", Byte.class));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", intNotByte), sqle.getMessage());
            
            sqle = expectThrows(SQLException.class, () -> results.getByte("test_short"));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", shortNotByte), sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_short", Byte.class));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", shortNotByte), sqle.getMessage());
            
            sqle = expectThrows(SQLException.class, () -> results.getByte("test_long"));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", Long.toString(longNotByte)), sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_long", Byte.class));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", Long.toString(longNotByte)), sqle.getMessage());
            
            sqle = expectThrows(SQLException.class, () -> results.getByte("test_double"));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", doubleErrorMessage), sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_double", Byte.class));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", doubleErrorMessage), sqle.getMessage());
            
            sqle = expectThrows(SQLException.class, () -> results.getByte("test_float"));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", Double.toString(floatNotByte)), sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_float", Byte.class));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", Double.toString(floatNotByte)), sqle.getMessage());
            
            sqle = expectThrows(SQLException.class, () -> results.getByte("test_keyword"));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [KEYWORD] to [Byte]", randomString),
                    sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_keyword", Byte.class));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [KEYWORD] to [Byte]", randomString),
                    sqle.getMessage());
            
            sqle = expectThrows(SQLException.class, () -> results.getByte("test_date"));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [DATETIME] to [Byte]", asDateString(randomDate)),
                    sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_date", Byte.class));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [DATETIME] to [Byte]", asDateString(randomDate)),
                    sqle.getMessage());
        });
    }
    
    // Short values testing
    public void testGettingValidShortWithoutCasting() throws Exception {
        short random1 = randomShort();
        short random2 = randomValueOtherThan(random1, () -> randomShort());
        short random3 = randomValueOtherThanMany(Arrays.asList(random1, random2)::contains, () -> randomShort());
        
        createTestDataForShortValueTests(random1, random2, random3);
        
        doWithQuery("SELECT test_short, test_null_short, test_keyword FROM test", (results) -> {
            ResultSetMetaData resultSetMetaData = results.getMetaData();

            results.next();
            assertEquals(3, resultSetMetaData.getColumnCount());
            assertEquals(Types.SMALLINT, resultSetMetaData.getColumnType(1));
            assertEquals(Types.SMALLINT, resultSetMetaData.getColumnType(2));
            assertEquals(random1, results.getShort(1));
            assertEquals(random1, results.getShort("test_short"));
            assertEquals(random1, results.getObject("test_short"));
            assertTrue(results.getObject(1) instanceof Short);
            
            assertEquals(0, results.getShort(2));
            assertTrue(results.wasNull());
            assertEquals(null, results.getObject("test_null_short"));
            assertTrue(results.wasNull());
            
            assertTrue(results.next());
            assertEquals(random2, results.getShort(1));
            assertEquals(random2, results.getShort("test_short"));
            assertTrue(results.getObject(1) instanceof Short);
            assertEquals(random3, results.getShort("test_keyword"));
            
            assertFalse(results.next());
        });
    }
    
    public void testGettingValidShortWithCasting() throws Exception {
        Map<String,Number> map = createTestDataForNumericValueTypes(() -> randomShort());
        
        doWithQuery(SELECT_WILDCARD, (results) -> {
            results.next();
            for(Entry<String, Number> e : map.entrySet()) {
                short actual = results.getObject(e.getKey(), Short.class);
                if (e.getValue() instanceof Double) {
                    assertEquals("For field " + e.getKey(), Math.round(e.getValue().doubleValue()), results.getShort(e.getKey()));
                    assertEquals("For field " + e.getKey(), Math.round(e.getValue().doubleValue()), actual);
                } else if (e.getValue() instanceof Float) {
                    assertEquals("For field " + e.getKey(), Math.round(e.getValue().floatValue()), results.getShort(e.getKey()));
                    assertEquals("For field " + e.getKey(), Math.round(e.getValue().floatValue()), actual);
                } else {
                    assertEquals("For field " + e.getKey(),
                            e.getValue().shortValue(), results.getShort(e.getKey()));
                    assertEquals("For field " + e.getKey(), e.getValue().shortValue(), actual);
                }
            }
        });
    }

    public void testGettingInvalidShort() throws Exception {
        createIndex("test");
        updateMappingForNumericValuesTests("test");
        updateMapping("test", builder -> {
            builder.startObject("test_keyword").field("type", "keyword").endObject();
            builder.startObject("test_date").field("type", "date").endObject();
        });
        
        int intNotShort = randomIntBetween(Short.MAX_VALUE + 1, Integer.MAX_VALUE);
        long longNotShort = randomLongBetween(Short.MAX_VALUE + 1, Long.MAX_VALUE);
        double doubleNotShort = randomDoubleBetween(Short.MAX_VALUE + 1, Double.MAX_VALUE, true);
        float floatNotShort = randomFloatBetween(Short.MAX_VALUE + 1, Float.MAX_VALUE);
        String randomString = randomUnicodeOfCodepointLengthBetween(128, 256);
        long randomDate = randomNonNegativeLong();
        
        String doubleErrorMessage = (doubleNotShort > Long.MAX_VALUE || doubleNotShort < Long.MIN_VALUE) ?
                Double.toString(doubleNotShort) : Long.toString(Math.round(doubleNotShort));

        index("test", "1", builder -> {
            builder.field("test_integer", intNotShort);
            builder.field("test_long", longNotShort);
            builder.field("test_double", doubleNotShort);
            builder.field("test_float", floatNotShort);
            builder.field("test_keyword", randomString);
            builder.field("test_date", randomDate);
        });
        
        doWithQuery(SELECT_WILDCARD, (results) -> {
            results.next();
            
            SQLException sqle = expectThrows(SQLException.class, () -> results.getShort("test_integer"));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", intNotShort), sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_integer", Short.class));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", intNotShort), sqle.getMessage());
            
            sqle = expectThrows(SQLException.class, () -> results.getShort("test_long"));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", Long.toString(longNotShort)), sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_long", Short.class));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", Long.toString(longNotShort)), sqle.getMessage());
            
            sqle = expectThrows(SQLException.class, () -> results.getShort("test_double"));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", doubleErrorMessage), sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_double", Short.class));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", doubleErrorMessage), sqle.getMessage());
            
            sqle = expectThrows(SQLException.class, () -> results.getShort("test_float"));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", Double.toString(floatNotShort)), sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_float", Short.class));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", Double.toString(floatNotShort)), sqle.getMessage());
            
            sqle = expectThrows(SQLException.class, () -> results.getShort("test_keyword"));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [KEYWORD] to [Short]", randomString),
                    sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_keyword", Short.class));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [KEYWORD] to [Short]", randomString),
                    sqle.getMessage());
            
            sqle = expectThrows(SQLException.class, () -> results.getShort("test_date"));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [DATETIME] to [Short]", asDateString(randomDate)),
                    sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_date", Short.class));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [DATETIME] to [Short]", asDateString(randomDate)),
                    sqle.getMessage());
        });
    }
    
    // Integer values testing
    public void testGettingValidIntegerWithoutCasting() throws Exception {
        int random1 = randomInt();
        int random2 = randomValueOtherThan(random1, () -> randomInt());
        int random3 = randomValueOtherThanMany(Arrays.asList(random1, random2)::contains, () -> randomInt());
        
        createTestDataForIntegerValueTests(random1, random2, random3);
        
        doWithQuery("SELECT test_integer,test_null_integer,test_keyword FROM test", (results) -> {
            ResultSetMetaData resultSetMetaData = results.getMetaData();

            results.next();
            assertEquals(3, resultSetMetaData.getColumnCount());
            assertEquals(Types.INTEGER, resultSetMetaData.getColumnType(1));
            assertEquals(Types.INTEGER, resultSetMetaData.getColumnType(2));
            assertEquals(random1, results.getInt(1));
            assertEquals(random1, results.getInt("test_integer"));
            assertEquals(random1, (int) results.getObject("test_integer", Integer.class));
            assertTrue(results.getObject(1) instanceof Integer);
            
            assertEquals(0, results.getInt(2));
            assertTrue(results.wasNull());
            assertEquals(null, results.getObject("test_null_integer"));
            assertTrue(results.wasNull());
            
            assertTrue(results.next());
            assertEquals(random2, results.getInt(1));
            assertEquals(random2, results.getInt("test_integer"));
            assertTrue(results.getObject(1) instanceof Integer);
            assertEquals(random3, results.getInt("test_keyword"));
            
            assertFalse(results.next());
        });
    }
    
    public void testGettingValidIntegerWithCasting() throws Exception {
        Map<String,Number> map = createTestDataForNumericValueTypes(() -> randomInt());
        
        doWithQuery(SELECT_WILDCARD, (results) -> {
            results.next();
            for(Entry<String, Number> e : map.entrySet()) {
                int actual = results.getObject(e.getKey(), Integer.class);
                if (e.getValue() instanceof Double) {
                    assertEquals("For field " + e.getKey(), Math.round(e.getValue().doubleValue()), results.getInt(e.getKey()));
                    assertEquals("For field " + e.getKey(), Math.round(e.getValue().doubleValue()), actual);
                } else if (e.getValue() instanceof Float) {
                    assertEquals("For field " + e.getKey(), e.getValue(), Integer.valueOf(results.getInt(e.getKey())).floatValue());
                    assertEquals("For field " + e.getKey(), e.getValue(), Integer.valueOf(actual).floatValue());
                } else {
                    assertEquals("For field " + e.getKey(), e.getValue().intValue(), results.getInt(e.getKey()));
                    assertEquals("For field " + e.getKey(), e.getValue().intValue(), actual);
                }
            }
        });
    }

    public void testGettingInvalidInteger() throws Exception {
        createIndex("test");
        updateMappingForNumericValuesTests("test");
        updateMapping("test", builder -> {
            builder.startObject("test_keyword").field("type", "keyword").endObject();
            builder.startObject("test_date").field("type", "date").endObject();
        });
        
        long longNotInt = randomLongBetween(getMaxIntPlusOne(), Long.MAX_VALUE);
        double doubleNotInt = randomDoubleBetween(getMaxIntPlusOne().doubleValue(), Double.MAX_VALUE, true);
        float floatNotInt = randomFloatBetween(getMaxIntPlusOne().floatValue(), Float.MAX_VALUE);
        String randomString = randomUnicodeOfCodepointLengthBetween(128, 256);
        long randomDate = randomNonNegativeLong();
        
        String doubleErrorMessage = (doubleNotInt > Long.MAX_VALUE || doubleNotInt < Long.MIN_VALUE) ?
                Double.toString(doubleNotInt) : Long.toString(Math.round(doubleNotInt));

        index("test", "1", builder -> {
            builder.field("test_long", longNotInt);
            builder.field("test_double", doubleNotInt);
            builder.field("test_float", floatNotInt);
            builder.field("test_keyword", randomString);
            builder.field("test_date", randomDate);
        });
        
        doWithQuery(SELECT_WILDCARD, (results) -> {
            results.next();
            
            SQLException sqle = expectThrows(SQLException.class, () -> results.getInt("test_long"));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", Long.toString(longNotInt)), sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_long", Integer.class));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", Long.toString(longNotInt)), sqle.getMessage());
            
            sqle = expectThrows(SQLException.class, () -> results.getInt("test_double"));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", doubleErrorMessage), sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_double", Integer.class));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", doubleErrorMessage), sqle.getMessage());
            
            sqle = expectThrows(SQLException.class, () -> results.getInt("test_float"));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", Double.toString(floatNotInt)), sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_float", Integer.class));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", Double.toString(floatNotInt)), sqle.getMessage());
            
            sqle = expectThrows(SQLException.class, () -> results.getInt("test_keyword"));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [KEYWORD] to [Integer]", randomString),
                    sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_keyword", Integer.class));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [KEYWORD] to [Integer]", randomString),
                    sqle.getMessage());
            
            sqle = expectThrows(SQLException.class, () -> results.getInt("test_date"));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [DATETIME] to [Integer]",
                asDateString(randomDate)), sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_date", Integer.class));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [DATETIME] to [Integer]",
                asDateString(randomDate)), sqle.getMessage());
        });
    }
    
    // Long values testing
    public void testGettingValidLongWithoutCasting() throws Exception {
        long random1 = randomLong();
        long random2 = randomValueOtherThan(random1, () -> randomLong());
        long random3 = randomValueOtherThanMany(Arrays.asList(random1, random2)::contains, () -> randomLong());
        
        createTestDataForLongValueTests(random1, random2, random3);
        
        doWithQuery("SELECT test_long, test_null_long, test_keyword FROM test", (results) -> {
            ResultSetMetaData resultSetMetaData = results.getMetaData();

            results.next();
            assertEquals(3, resultSetMetaData.getColumnCount());
            assertEquals(Types.BIGINT, resultSetMetaData.getColumnType(1));
            assertEquals(Types.BIGINT, resultSetMetaData.getColumnType(2));
            assertEquals(random1, results.getLong(1));
            assertEquals(random1, results.getLong("test_long"));
            assertEquals(random1, (long) results.getObject("test_long", Long.class));
            assertTrue(results.getObject(1) instanceof Long);
            
            assertEquals(0, results.getLong(2));
            assertTrue(results.wasNull());
            assertEquals(null, results.getObject("test_null_long"));
            assertTrue(results.wasNull());
            
            assertTrue(results.next());
            assertEquals(random2, results.getLong(1));
            assertEquals(random2, results.getLong("test_long"));
            assertTrue(results.getObject(1) instanceof Long);
            assertEquals(random3, results.getLong("test_keyword"));
            
            assertFalse(results.next());
        });
    }
    
    public void testGettingValidLongWithCasting() throws Exception {
        Map<String,Number> map = createTestDataForNumericValueTypes(() -> randomLong());
        
        doWithQuery(SELECT_WILDCARD, (results) -> {
            results.next();
            for(Entry<String, Number> e : map.entrySet()) {
                long actual = results.getObject(e.getKey(), Long.class);
                if (e.getValue() instanceof Float) {
                    assertEquals("For field " + e.getKey(), e.getValue(), Long.valueOf(results.getLong(e.getKey())).floatValue());
                    assertEquals("For field " + e.getKey(), e.getValue(), Long.valueOf(actual).floatValue());
                } else {
                    assertEquals("For field " + e.getKey(), e.getValue().longValue(), results.getLong(e.getKey()));
                    assertEquals("For field " + e.getKey(), e.getValue().longValue(), actual);
                }
            }
        });
    }

    public void testGettingInvalidLong() throws Exception {
        createIndex("test");
        updateMappingForNumericValuesTests("test");
        updateMapping("test", builder -> {
            builder.startObject("test_keyword").field("type", "keyword").endObject();
            builder.startObject("test_date").field("type", "date").endObject();
        });
        
        double doubleNotLong = randomDoubleBetween(getMaxLongPlusOne().doubleValue(), Double.MAX_VALUE, true);
        float floatNotLong = randomFloatBetween(getMaxLongPlusOne().floatValue(), Float.MAX_VALUE);
        String randomString = randomUnicodeOfCodepointLengthBetween(128, 256);
        long randomDate = randomNonNegativeLong();

        index("test", "1", builder -> {
            builder.field("test_double", doubleNotLong);
            builder.field("test_float", floatNotLong);
            builder.field("test_keyword", randomString);
            builder.field("test_date", randomDate);
        });
        
        doWithQuery(SELECT_WILDCARD, (results) -> {
            results.next();
            
            SQLException sqle = expectThrows(SQLException.class, () -> results.getLong("test_double"));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", Double.toString(doubleNotLong)), sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_double", Long.class));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", Double.toString(doubleNotLong)), sqle.getMessage());
            
            sqle = expectThrows(SQLException.class, () -> results.getLong("test_float"));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", Double.toString(floatNotLong)), sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_float", Long.class));
            assertEquals(format(Locale.ROOT, "Numeric %s out of range", Double.toString(floatNotLong)), sqle.getMessage());
            
            sqle = expectThrows(SQLException.class, () -> results.getLong("test_keyword"));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [KEYWORD] to [Long]", randomString),
                    sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_keyword", Long.class));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [KEYWORD] to [Long]", randomString),
                    sqle.getMessage());
            
            sqle = expectThrows(SQLException.class, () -> results.getLong("test_date"));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [DATETIME] to [Long]", asDateString(randomDate)),
                    sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_date", Long.class));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [DATETIME] to [Long]", asDateString(randomDate)),
                    sqle.getMessage());
        });
    }
    
    // Double values testing
    public void testGettingValidDoubleWithoutCasting() throws Exception {
        double random1 = randomDouble();
        double random2 = randomValueOtherThan(random1, () -> randomDouble());
        double random3 = randomValueOtherThanMany(Arrays.asList(random1, random2)::contains, () -> randomDouble());
        
        createTestDataForDoubleValueTests(random1, random2, random3);
        
        doWithQuery("SELECT test_double, test_null_double, test_keyword FROM test", (results) -> {
            ResultSetMetaData resultSetMetaData = results.getMetaData();

            results.next();
            assertEquals(3, resultSetMetaData.getColumnCount());
            assertEquals(Types.DOUBLE, resultSetMetaData.getColumnType(1));
            assertEquals(Types.DOUBLE, resultSetMetaData.getColumnType(2));
            assertEquals(random1, results.getDouble(1), 0.0d);
            assertEquals(random1, results.getDouble("test_double"), 0.0d);
            assertEquals(random1, results.getObject("test_double", Double.class), 0.0d);
            assertTrue(results.getObject(1) instanceof Double);
            
            assertEquals(0, results.getDouble(2), 0.0d);
            assertTrue(results.wasNull());
            assertEquals(null, results.getObject("test_null_double"));
            assertTrue(results.wasNull());
            
            assertTrue(results.next());
            assertEquals(random2, results.getDouble(1), 0.0d);
            assertEquals(random2, results.getDouble("test_double"), 0.0d);
            assertTrue(results.getObject(1) instanceof Double);
            assertEquals(random3, results.getDouble("test_keyword"), 0.0d);
            
            assertFalse(results.next());
        });
    }
    
    public void testGettingValidDoubleWithCasting() throws Exception {
        Map<String,Number> map = createTestDataForNumericValueTypes(() -> randomDouble());
        
        doWithQuery(SELECT_WILDCARD, (results) -> {
            results.next();
            for(Entry<String, Number> e : map.entrySet()) {
                if (e.getValue() instanceof Float) {
                    assertEquals("For field " + e.getKey(), e.getValue(), Double.valueOf(results.getDouble(e.getKey())).floatValue());
                    assertEquals("For field " + e.getKey(),
                            e.getValue(), Double.valueOf(results.getObject(e.getKey(), Double.class)).floatValue());
                } else {
                    assertEquals("For field " + e.getKey(), e.getValue().doubleValue(), results.getDouble(e.getKey()), 0.0d);
                    assertEquals("For field " + e.getKey(),
                            e.getValue().doubleValue(), results.getObject(e.getKey(), Double.class), 0.0d);
                }
            }
        });
    }

    public void testGettingInvalidDouble() throws Exception {
        createIndex("test");
        updateMappingForNumericValuesTests("test");
        updateMapping("test", builder -> {
            builder.startObject("test_keyword").field("type", "keyword").endObject();
            builder.startObject("test_date").field("type", "date").endObject();
        });
        
        String randomString = randomUnicodeOfCodepointLengthBetween(128, 256);
        long randomDate = randomNonNegativeLong();

        index("test", "1", builder -> {
            builder.field("test_keyword", randomString);
            builder.field("test_date", randomDate);
        });
        
        doWithQuery(SELECT_WILDCARD, (results) -> {
            results.next();
            
            SQLException sqle = expectThrows(SQLException.class, () -> results.getDouble("test_keyword"));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [KEYWORD] to [Double]", randomString),
                    sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_keyword", Double.class));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [KEYWORD] to [Double]", randomString),
                    sqle.getMessage());
            
            sqle = expectThrows(SQLException.class, () -> results.getDouble("test_date"));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [DATETIME] to [Double]", asDateString(randomDate)),
                    sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_date", Double.class));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [DATETIME] to [Double]", asDateString(randomDate)),
                    sqle.getMessage());
        });
    }
    
    // Float values testing
    public void testGettingValidFloatWithoutCasting() throws Exception {
        float random1 = randomFloat();
        float random2 = randomValueOtherThan(random1, () -> randomFloat());
        float random3 = randomValueOtherThanMany(Arrays.asList(random1, random2)::contains, () -> randomFloat());
        
        createTestDataForFloatValueTests(random1, random2, random3);
        
        doWithQuery("SELECT test_float, test_null_float, test_keyword FROM test", (results) -> {
            ResultSetMetaData resultSetMetaData = results.getMetaData();
    
            results.next();
            assertEquals(3, resultSetMetaData.getColumnCount());
            assertEquals(Types.REAL, resultSetMetaData.getColumnType(1));
            assertEquals(Types.REAL, resultSetMetaData.getColumnType(2));
            assertEquals(random1, results.getFloat(1), 0.0f);
            assertEquals(random1, results.getFloat("test_float"), 0.0f);
            assertEquals(random1, results.getObject("test_float", Float.class), 0.0f);
            assertTrue(results.getObject(1) instanceof Float);
            
            assertEquals(0, results.getFloat(2), 0.0d);
            assertTrue(results.wasNull());
            assertEquals(null, results.getObject("test_null_float"));
            assertTrue(results.wasNull());
            
            assertTrue(results.next());
            assertEquals(random2, results.getFloat(1), 0.0d);
            assertEquals(random2, results.getFloat("test_float"), 0.0d);
            assertTrue(results.getObject(1) instanceof Float);
            assertEquals(random3, results.getFloat("test_keyword"), 0.0d);
            
            assertFalse(results.next());
        });
    }
    
    public void testGettingValidFloatWithCasting() throws Exception {
        Map<String,Number> map = createTestDataForNumericValueTypes(() -> randomFloat());
        
        doWithQuery(SELECT_WILDCARD, (results) -> {
            results.next();
            for(Entry<String, Number> e : map.entrySet()) {
                assertEquals("For field " + e.getKey(), e.getValue().floatValue(), results.getFloat(e.getKey()), 0.0f);
                assertEquals("For field " + e.getKey(),
                        e.getValue().floatValue(), results.getObject(e.getKey(), Float.class), 0.0f);
            }
        });
    }

    public void testGettingInvalidFloat() throws Exception {
        createIndex("test");
        updateMappingForNumericValuesTests("test");
        updateMapping("test", builder -> {
            builder.startObject("test_keyword").field("type", "keyword").endObject();
            builder.startObject("test_date").field("type", "date").endObject();
        });
        
        String randomString = randomUnicodeOfCodepointLengthBetween(128, 256);
        long randomDate = randomNonNegativeLong();

        index("test", "1", builder -> {
            builder.field("test_keyword", randomString);
            builder.field("test_date", randomDate);
        });
        
        doWithQuery(SELECT_WILDCARD, (results) -> {
            results.next();

            SQLException sqle = expectThrows(SQLException.class, () -> results.getFloat("test_keyword"));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [KEYWORD] to [Float]", randomString),
                    sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_keyword", Float.class));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [KEYWORD] to [Float]", randomString),
                    sqle.getMessage());
            
            sqle = expectThrows(SQLException.class, () -> results.getFloat("test_date"));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [DATETIME] to [Float]", asDateString(randomDate)),
                    sqle.getMessage());
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_date", Float.class));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [DATETIME] to [Float]", asDateString(randomDate)),
                    sqle.getMessage());
        });
    }
    
    public void testGettingBooleanValues() throws Exception {
        createIndex("test");
        updateMappingForNumericValuesTests("test");
        updateMapping("test", builder -> {
            builder.startObject("test_boolean").field("type", "boolean").endObject();
            builder.startObject("test_date").field("type", "date").endObject();
        });
        long randomDate1 = randomNonNegativeLong();
        long randomDate2 = randomNonNegativeLong();
        
        // true values
        indexSimpleDocumentWithTrueValues(randomDate1);
        
        // false values
        index("test", "2", builder -> {
            builder.field("test_boolean", false);
            builder.field("test_byte", 0);
            builder.field("test_integer", 0);
            builder.field("test_long", 0L);
            builder.field("test_short", 0);
            builder.field("test_double", 0d);
            builder.field("test_float", 0f);
            builder.field("test_keyword", "false");
            builder.field("test_date", randomDate2);
        });
        
        // other (non 0 = true) values
        index("test", "3", builder -> {
            builder.field("test_byte", randomValueOtherThan((byte) 0, () -> randomByte()));
            builder.field("test_integer", randomValueOtherThan(0, () -> randomInt()));
            builder.field("test_long", randomValueOtherThan(0L, () -> randomLong()));
            builder.field("test_short", randomValueOtherThan((short) 0, () -> randomShort()));
            builder.field("test_double", randomValueOtherThanMany(i -> i < 1.0d && i > -1.0d && i < Double.MAX_VALUE
                    && i > Double.MIN_VALUE,
                    () -> randomDouble() * randomInt()));
            builder.field("test_float", randomValueOtherThanMany(i -> i < 1.0f && i > -1.0f && i < Float.MAX_VALUE && i > Float.MIN_VALUE,
                    () -> randomFloat() * randomInt()));
            builder.field("test_keyword", "1");
        });
        
        // other false values
        index("test", "4", builder -> {
            builder.field("test_keyword", "0");
        });
        
        doWithQuery(SELECT_WILDCARD, (results) -> {
            results.next();
            assertEquals(true, results.getBoolean("test_boolean"));
            for(String fld : fieldsNames) {
                assertEquals("Expected: <true> but was: <false> for field " + fld, true, results.getBoolean(fld));
                assertEquals("Expected: <true> but was: <false> for field " + fld, true, results.getObject(fld, Boolean.class));
            }
            SQLException sqle = expectThrows(SQLException.class, () -> results.getBoolean("test_date"));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [DATETIME] to [Boolean]",
                asDateString(randomDate1)), sqle.getMessage());
            
            results.next();
            assertEquals(false, results.getBoolean("test_boolean"));
            for(String fld : fieldsNames) {
                assertEquals("Expected: <false> but was: <true> for field " + fld, false, results.getBoolean(fld));
                assertEquals("Expected: <false> but was: <true> for field " + fld, false, results.getObject(fld, Boolean.class));
            }
            sqle = expectThrows(SQLException.class, () -> results.getBoolean("test_date"));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [DATETIME] to [Boolean]",
                asDateString(randomDate2)), sqle.getMessage());
            
            sqle = expectThrows(SQLException.class, () -> results.getObject("test_date", Boolean.class));
            assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [DATETIME] to [Boolean]",
                asDateString(randomDate2)), sqle.getMessage());
            
            results.next();
            for(String fld : fieldsNames.stream()
                    .filter((f) -> !f.equals("test_keyword")).collect(Collectors.toCollection(HashSet::new))) {
                assertEquals("Expected: <true> but was: <false> for field " + fld, true, results.getBoolean(fld));
                assertEquals("Expected: <true> but was: <false> for field " + fld, true, results.getObject(fld, Boolean.class));
            }
            
            results.next();
            assertEquals(false, results.getBoolean("test_keyword"));
            assertEquals(false, results.getObject("test_keyword", Boolean.class));
        });
    }
    
    public void testGettingDateWithoutCalendar() throws Exception {
        createIndex("test");
        updateMappingForNumericValuesTests("test");
        updateMapping("test", builder -> {
            builder.startObject("test_boolean").field("type", "boolean").endObject();
            builder.startObject("test_date").field("type", "date").endObject();
        });
        Long randomLongDate = randomNonNegativeLong();
        indexSimpleDocumentWithTrueValues(randomLongDate);
        
        doWithQuery(SELECT_ALL_FIELDS, (results) -> {
            results.next();

            java.sql.Date expectedDate = asDate(randomLongDate, getZoneFromOffset(randomLongDate));

            assertEquals(expectedDate, results.getDate("test_date"));
            assertEquals(expectedDate, results.getDate(9));
            assertEquals(expectedDate, results.getObject("test_date", java.sql.Date.class));
            assertEquals(expectedDate, results.getObject(9, java.sql.Date.class));

            // bulk validation for all fields which are not of type date
            validateErrorsForDateTestsWithoutCalendar(results::getDate);
        });
    }
    
    public void testGettingDateWithCalendar() throws Exception {
        createIndex("test");
        updateMappingForNumericValuesTests("test");
        updateMapping("test", builder -> {
            builder.startObject("test_boolean").field("type", "boolean").endObject();
            builder.startObject("test_date").field("type", "date").endObject();
        });
        Long randomLongDate = randomNonNegativeLong();
        indexSimpleDocumentWithTrueValues(randomLongDate);
        index("test", "2", builder -> {
            builder.timeField("test_date", null);
        });
        
        String anotherTZId = randomValueOtherThan(timeZoneId, () -> randomKnownTimeZone());
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone(anotherTZId), Locale.ROOT);
        
        doWithQuery(SELECT_ALL_FIELDS, (results) -> {
            results.next();
            c.setTimeInMillis(randomLongDate);
            c.set(HOUR_OF_DAY, 0);
            c.set(MINUTE, 0);
            c.set(SECOND, 0);
            c.set(MILLISECOND, 0);

            assertEquals(results.getDate("test_date", c), new java.sql.Date(c.getTimeInMillis()));
            assertEquals(results.getDate(9, c), new java.sql.Date(c.getTimeInMillis()));
            
            // bulk validation for all fields which are not of type date
            validateErrorsForDateTimeTestsWithCalendar(c, results::getDate);
            
            results.next();
            assertNull(results.getDate("test_date"));
        });
    }
    
    public void testGettingTimeWithoutCalendar() throws Exception {
        createIndex("test");
        updateMappingForNumericValuesTests("test");
        updateMapping("test", builder -> {
            builder.startObject("test_boolean").field("type", "boolean").endObject();
            builder.startObject("test_date").field("type", "date").endObject();
        });
        Long randomLongDate = randomNonNegativeLong();
        indexSimpleDocumentWithTrueValues(randomLongDate);

        doWithQuery(SELECT_ALL_FIELDS, (results) -> {
            results.next();

            java.sql.Time expectedTime = asTime(randomLongDate, getZoneFromOffset(randomLongDate));

            assertEquals(expectedTime, results.getTime("test_date"));
            assertEquals(expectedTime, results.getTime(9));
            assertEquals(expectedTime, results.getObject("test_date", java.sql.Time.class));
            assertEquals(expectedTime, results.getObject(9, java.sql.Time.class));

            validateErrorsForTimeTestsWithoutCalendar(results::getTime);
        });
    }

    public void testGettingTimeWithCalendar() throws Exception {
        createIndex("test");
        updateMappingForNumericValuesTests("test");
        updateMapping("test", builder -> {
            builder.startObject("test_boolean").field("type", "boolean").endObject();
            builder.startObject("test_date").field("type", "date").endObject();
        });
        Long randomLongDate = randomNonNegativeLong();
        indexSimpleDocumentWithTrueValues(randomLongDate);
        index("test", "2", builder -> {
            builder.timeField("test_date", null);
        });
        
        String anotherTZId = randomValueOtherThan(timeZoneId, () -> randomKnownTimeZone());
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone(anotherTZId), Locale.ROOT);
        
        doWithQuery(SELECT_ALL_FIELDS, (results) -> {
            results.next();
            c.setTimeInMillis(randomLongDate);
            c.set(ERA, GregorianCalendar.AD);
            c.set(YEAR, 1970);
            c.set(MONTH, 0);
            c.set(DAY_OF_MONTH, 1);
            
            assertEquals(results.getTime("test_date", c), new java.sql.Time(c.getTimeInMillis()));
            assertEquals(results.getTime(9, c), new java.sql.Time(c.getTimeInMillis()));
            
            validateErrorsForDateTimeTestsWithCalendar(c, results::getTime);
            
            results.next();
            assertNull(results.getTime("test_date"));
        });
    }
    
    public void testGettingTimestampWithoutCalendar() throws Exception {
        createIndex("library");
        updateMapping("library", builder -> {
            builder.startObject("release_date").field("type", "date").endObject();
            builder.startObject("republish_date").field("type", "date").endObject();
        });
        long randomMillis = randomNonNegativeLong();

        index("library", "1", builder -> {
            builder.field("name", "Don Quixote");
            builder.field("page_count", 1072);
            builder.field("release_date", randomMillis);
            builder.timeField("republish_date", null);
        });
        index("library", "2", builder -> {
            builder.field("name", "1984");
            builder.field("page_count", 328);
            builder.field("release_date", 649036800000L);
            builder.field("republish_date", 599616000000L);
        });

        doWithQuery("SELECT name, release_date, republish_date FROM library", (results) -> {
            ResultSetMetaData resultSetMetaData = results.getMetaData();

            results.next();
            assertEquals(3, resultSetMetaData.getColumnCount());
            assertEquals(randomMillis, results.getTimestamp("release_date").getTime());
            assertEquals(randomMillis, results.getTimestamp(2).getTime());
            assertTrue(results.getObject(2) instanceof Timestamp);
            assertEquals(randomMillis, ((Timestamp) results.getObject("release_date")).getTime());
            
            assertNull(results.getTimestamp(3));
            assertNull(results.getObject("republish_date"));

            assertTrue(results.next());
            assertEquals(599616000000L, results.getTimestamp("republish_date").getTime());
            assertEquals(649036800000L, ((Timestamp) results.getObject(2)).getTime());

            assertFalse(results.next());
        });
    }
    
    public void testGettingTimestampWithCalendar() throws Exception {
        createIndex("test");
        updateMappingForNumericValuesTests("test");
        updateMapping("test", builder -> {
            builder.startObject("test_boolean").field("type", "boolean").endObject();
            builder.startObject("test_date").field("type", "date").endObject();
        });
        Long randomLongDate = randomNonNegativeLong();
        indexSimpleDocumentWithTrueValues(randomLongDate);
        index("test", "2", builder -> {
            builder.timeField("test_date", null);
        });
        
        String anotherTZId = randomValueOtherThan(timeZoneId, () -> randomKnownTimeZone());
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone(anotherTZId), Locale.ROOT);
        
        doWithQuery(SELECT_ALL_FIELDS, (results) -> {
            results.next();
            c.setTimeInMillis(randomLongDate);
            
            assertEquals(results.getTimestamp("test_date", c), new java.sql.Timestamp(c.getTimeInMillis()));
            assertEquals(results.getTimestamp(9, c), new java.sql.Timestamp(c.getTimeInMillis()));
            
            validateErrorsForDateTimeTestsWithCalendar(c, results::getTimestamp);

            results.next();
            assertNull(results.getTimestamp("test_date"));
        });
    }

    public void testScalarOnDates() throws Exception {
        createIndex("test");
        updateMapping("test", builder -> builder.startObject("test_date").field("type", "date").endObject());

        // 2018-03-12 17:00:00 UTC
        Long dateInMillis = 1520874000000L;
        index("test", "1", builder -> builder.field("test_date", dateInMillis));

        // UTC +10 hours
        String timeZoneId1 = "Etc/GMT-10";
        Calendar connCalendar1 = Calendar.getInstance(TimeZone.getTimeZone(timeZoneId1), Locale.ROOT);

        doWithQueryAndTimezone("SELECT test_date, DAY_OF_MONTH(test_date) as day FROM test", timeZoneId1, results -> {
            results.next();
            connCalendar1.setTimeInMillis(dateInMillis);
            connCalendar1.set(HOUR_OF_DAY, 0);
            connCalendar1.set(MINUTE, 0);
            connCalendar1.set(SECOND, 0);
            connCalendar1.set(MILLISECOND, 0);

            java.sql.Date expectedDate = new java.sql.Date(connCalendar1.getTimeInMillis());
            assertEquals(expectedDate, results.getDate("test_date"));
            assertEquals(expectedDate, results.getDate(1));
            assertEquals(expectedDate, results.getObject("test_date", java.sql.Date.class));
            assertEquals(expectedDate, results.getObject(1, java.sql.Date.class));

            // +1 day
            assertEquals(13, results.getInt("day"));
        });

        delete("test", "1");

        // 2018-03-12 05:00:00 UTC
        Long dateInMillis2 = 1520830800000L;
        index("test", "1", builder -> builder.field("test_date", dateInMillis2));

        // UTC -10 hours
        String timeZoneId2 = "Etc/GMT+10";
        Calendar connCalendar2 = Calendar.getInstance(TimeZone.getTimeZone(timeZoneId2), Locale.ROOT);


        doWithQueryAndTimezone("SELECT test_date, DAY_OF_MONTH(test_date) as day FROM test", timeZoneId2, results -> {
            results.next();
            connCalendar2.setTimeInMillis(dateInMillis2);
            connCalendar2.set(HOUR_OF_DAY, 0);
            connCalendar2.set(MINUTE, 0);
            connCalendar2.set(SECOND, 0);
            connCalendar2.set(MILLISECOND, 0);

            java.sql.Date expectedDate = new java.sql.Date(connCalendar2.getTimeInMillis());
            assertEquals(expectedDate, results.getDate("test_date"));
            assertEquals(expectedDate, results.getDate(1));
            assertEquals(expectedDate, results.getObject("test_date", java.sql.Date.class));
            assertEquals(expectedDate, results.getObject(1, java.sql.Date.class));

            // -1 day
            assertEquals(11, results.getInt("day"));
        });
    }

    public void testGetDateType() throws Exception {
        createIndex("test");
        updateMapping("test", builder -> builder.startObject("test_date").field("type", "date").endObject());

        // 2018-03-12 17:00:00 UTC
        Long timeInMillis = 1520874000123L;
        index("test", "1", builder -> builder.field("test_date", timeInMillis));

        // UTC +10 hours
        String timeZoneId1 = "Etc/GMT-10";
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone(timeZoneId1), Locale.ROOT);

        doWithQueryAndTimezone("SELECT CAST(test_date AS DATE) as date FROM test", timeZoneId1, results -> {
            results.next();
            c.setTimeInMillis(timeInMillis);
            c.set(HOUR_OF_DAY, 0);
            c.set(MINUTE, 0);
            c.set(SECOND, 0);
            c.set(MILLISECOND, 0);

            java.sql.Date expectedDate = new java.sql.Date(c.getTimeInMillis());
            assertEquals(expectedDate, results.getDate("date"));
            assertEquals(expectedDate, results.getObject("date", java.sql.Date.class));

            java.sql.Time expectedTime = new java.sql.Time(0L);
            assertEquals(expectedTime, results.getTime("date"));
            assertEquals(expectedTime, results.getObject("date", java.sql.Time.class));

            java.sql.Timestamp expectedTimestamp = new java.sql.Timestamp(c.getTimeInMillis());
            assertEquals(expectedTimestamp, results.getTimestamp("date"));
            assertEquals(expectedTimestamp, results.getObject("date", java.sql.Timestamp.class));
        });
    }
    
    public void testGetDateTypeFromAggregation() throws Exception {
        createIndex("test");
        updateMapping("test", builder -> builder.startObject("test_date").field("type", "date").endObject());

        // 1984-05-02 14:59:12 UTC
        Long timeInMillis = 452357952000L;
        index("test", "1", builder -> builder.field("test_date", timeInMillis));

        doWithQueryAndTimezone("SELECT CONVERT(test_date, DATE) AS converted FROM test GROUP BY converted", "UTC", results -> {
            results.next();
            ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timeInMillis), ZoneId.of("Z"))
                    .toLocalDate().atStartOfDay(ZoneId.of("Z"));

            java.sql.Date expectedDate = new java.sql.Date(zdt.toInstant().toEpochMilli());
            assertEquals(expectedDate, results.getDate("converted"));
            assertEquals(expectedDate, results.getObject("converted", java.sql.Date.class));

            java.sql.Time expectedTime = new java.sql.Time(0L);
            assertEquals(expectedTime, results.getTime("converted"));
            assertEquals(expectedTime, results.getObject("converted", java.sql.Time.class));

            java.sql.Timestamp expectedTimestamp = new java.sql.Timestamp(zdt.toInstant().toEpochMilli());
            assertEquals(expectedTimestamp, results.getTimestamp("converted"));
            assertEquals(expectedTimestamp, results.getObject("converted", java.sql.Timestamp.class));
        });
    }

    public void testGetTimeType() throws Exception {
        createIndex("test");
        updateMapping("test", builder -> builder.startObject("test_date").field("type", "date").endObject());

        // 2018-03-12 17:20:30.123 UTC
        Long timeInMillis = 1520875230123L;
        index("test", "1", builder -> builder.field("test_date", timeInMillis));

        // UTC +10 hours
        String timeZoneId1 = "Etc/GMT-10";

        doWithQueryAndTimezone("SELECT CAST(test_date AS TIME) as time FROM test", timeZoneId1, results -> {
            results.next();

            java.sql.Date expectedDate = new java.sql.Date(0L);
            assertEquals(expectedDate, results.getDate("time"));
            assertEquals(expectedDate, results.getObject("time", java.sql.Date.class));

            java.sql.Time expectedTime = asTime(timeInMillis, ZoneId.of("Etc/GMT-10"));
            assertEquals(expectedTime, results.getTime("time"));
            assertEquals(expectedTime, results.getObject("time", java.sql.Time.class));

            java.sql.Timestamp expectedTimestamp = new java.sql.Timestamp(expectedTime.getTime());
            assertEquals(expectedTimestamp, results.getTimestamp("time"));
            assertEquals(expectedTimestamp, results.getObject("time", java.sql.Timestamp.class));
        });
    }

    public void testValidGetObjectCalls() throws Exception {
        createIndex("test");
        updateMappingForNumericValuesTests("test");
        updateMapping("test", builder -> {
            builder.startObject("test_boolean").field("type", "boolean").endObject();
            builder.startObject("test_date").field("type", "date").endObject();
        });
        
        byte b = randomByte();
        int i = randomInt();
        long l = randomLong();
        short s = (short) randomIntBetween(Short.MIN_VALUE, Short.MAX_VALUE);
        double d = randomDouble();
        float f = randomFloat();
        boolean randomBool = randomBoolean();
        Long randomLongDate = randomNonNegativeLong();
        String randomString = randomUnicodeOfCodepointLengthBetween(128, 256);
                
        index("test", "1", builder -> {
            builder.field("test_byte", b);
            builder.field("test_integer", i);
            builder.field("test_long", l);
            builder.field("test_short", s);
            builder.field("test_double", d);
            builder.field("test_float", f);
            builder.field("test_keyword", randomString);
            builder.field("test_date", randomLongDate);
            builder.field("test_boolean", randomBool);
        });
        
        doWithQuery(SELECT_WILDCARD, (results) -> {
            results.next();
            
            assertEquals(b, results.getObject("test_byte"));
            assertTrue(results.getObject("test_byte") instanceof Byte);
            
            assertEquals(i, results.getObject("test_integer"));
            assertTrue(results.getObject("test_integer") instanceof Integer);
            
            assertEquals(l, results.getObject("test_long"));
            assertTrue(results.getObject("test_long") instanceof Long);
            
            assertEquals(s, results.getObject("test_short"));
            assertTrue(results.getObject("test_short") instanceof Short);
            
            assertEquals(d, results.getObject("test_double"));
            assertTrue(results.getObject("test_double") instanceof Double);
            
            assertEquals(f, results.getObject("test_float"));
            assertTrue(results.getObject("test_float") instanceof Float);
            
            assertEquals(randomString, results.getObject("test_keyword"));
            assertTrue(results.getObject("test_keyword") instanceof String);
            
            assertEquals(new Date(randomLongDate), results.getObject("test_date"));
            assertTrue(results.getObject("test_date") instanceof Timestamp);
            
            assertEquals(randomBool, results.getObject("test_boolean"));
            assertTrue(results.getObject("test_boolean") instanceof Boolean);
        });
    }
    
    public void testGettingNullValues() throws Exception {
        String query = "SELECT CAST(NULL AS BOOLEAN) b, CAST(NULL AS TINYINT) t, CAST(NULL AS SMALLINT) s, CAST(NULL AS INTEGER) i,"
                + "CAST(NULL AS BIGINT) bi, CAST(NULL AS DOUBLE) d, CAST(NULL AS REAL) r, CAST(NULL AS FLOAT) f, CAST(NULL AS VARCHAR) v,"
                + "CAST(NULL AS DATE) dt, CAST(NULL AS TIME) tm, CAST(NULL AS TIMESTAMP) ts";
        doWithQuery(query, (results) -> {
            results.next();
            
            assertNull(results.getObject("b"));
            assertFalse(results.getBoolean("b"));
            
            assertNull(results.getObject("t"));
            assertEquals(0, results.getByte("t"));
            
            assertNull(results.getObject("s"));
            assertEquals(0, results.getShort("s"));
            
            assertNull(results.getObject("i"));
            assertEquals(0, results.getInt("i"));
            
            assertNull(results.getObject("bi"));
            assertEquals(0, results.getLong("bi"));
            
            assertNull(results.getObject("d"));
            assertEquals(0.0d, results.getDouble("d"), 0d);
            
            assertNull(results.getObject("r"));
            assertEquals(0.0f, results.getFloat("r"), 0f);
            
            assertNull(results.getObject("f"));
            assertEquals(0.0f, results.getFloat("f"), 0f);
            
            assertNull(results.getObject("v"));
            assertNull(results.getString("v"));
            
            assertNull(results.getObject("dt"));
            assertNull(results.getDate("dt"));
            assertNull(results.getDate("dt", randomCalendar()));
            
            assertNull(results.getObject("tm"));
            assertNull(results.getTime("tm"));
            assertNull(results.getTime("tm", randomCalendar()));
            
            assertNull(results.getObject("ts"));
            assertNull(results.getTimestamp("ts"));
            assertNull(results.getTimestamp("ts", randomCalendar()));
        });
    }

    /*
     * Checks StackOverflowError fix for https://github.com/elastic/elasticsearch/pull/31735
     */
    public void testNoInfiniteRecursiveGetObjectCalls() throws Exception {
        index("library", "1", builder -> {
            builder.field("name", "Don Quixote");
            builder.field("page_count", 1072);
        });
        Connection conn = esJdbc();
        PreparedStatement statement = conn.prepareStatement("SELECT * FROM library");
        ResultSet results = statement.executeQuery();

        try {
            results.next();
            results.getObject("name");
            results.getObject("page_count");
            results.getObject(1);
            results.getObject(1, String.class);
            results.getObject("page_count", Integer.class);
        } catch (StackOverflowError soe) {
            fail("Infinite recursive call on getObject() method");
        }
    }
    
    public void testUnsupportedGetMethods() throws IOException, SQLException {
        index("test", "1", builder -> {
            builder.field("test", "test");
        });
        Connection conn = esJdbc();
        PreparedStatement statement = conn.prepareStatement("SELECT * FROM test");
        ResultSet r = statement.executeQuery();
        
        r.next();
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getAsciiStream("test"), "AsciiStream not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getAsciiStream(1), "AsciiStream not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getArray("test"), "Array not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getArray(1), "Array not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getBigDecimal("test"), "BigDecimal not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getBigDecimal("test"), "BigDecimal not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getBinaryStream("test"), "BinaryStream not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getBinaryStream(1), "BinaryStream not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getBlob("test"), "Blob not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getBlob(1), "Blob not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getCharacterStream("test"), "CharacterStream not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getCharacterStream(1), "CharacterStream not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getClob("test"), "Clob not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getClob(1), "Clob not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getNCharacterStream("test"), "NCharacterStream not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getNCharacterStream(1), "NCharacterStream not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getNClob("test"), "NClob not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getNClob(1), "NClob not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getNString("test"), "NString not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getNString(1), "NString not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getRef("test"), "Ref not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getRef(1), "Ref not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getRowId("test"), "RowId not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getRowId(1), "RowId not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getSQLXML("test"), "SQLXML not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getSQLXML(1), "SQLXML not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getURL("test"), "URL not supported");
        assertThrowsUnsupportedAndExpectErrorMessage(() -> r.getURL(1), "URL not supported");
    }
    
    public void testUnsupportedUpdateMethods() throws IOException, SQLException {
        index("test", "1", builder -> {
            builder.field("test", "test");
        });
        Connection conn = esJdbc();
        PreparedStatement statement = conn.prepareStatement("SELECT * FROM test");
        ResultSet r = statement.executeQuery();
        
        r.next();
        Blob b = null;
        InputStream i = null;
        Clob c = null;
        NClob nc = null;
        Reader rd = null;
        
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateBytes(1, null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateBytes("", null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateArray(1, null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateArray("", null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateAsciiStream(1, null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateAsciiStream("", null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateAsciiStream(1, null, 1));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateAsciiStream(1, null, 1));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateAsciiStream("", null, 1));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateAsciiStream("", null, 1L));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateBigDecimal(1, null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateBigDecimal("", null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateBinaryStream(1, null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateBinaryStream("", null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateBinaryStream(1, null, 1));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateBinaryStream(1, null, 1));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateBinaryStream("", null, 1));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateBinaryStream("", null, 1L));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateBlob(1, b));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateBlob(1, i));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateBlob("", b));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateBlob("", i));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateBlob(1, null, 1));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateBlob("", null, 1));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateBoolean(1, false));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateBoolean("", false));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateByte(1, (byte) 1));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateByte("", (byte) 1));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateCharacterStream(1, null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateCharacterStream("", null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateCharacterStream(1, null, 1));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateCharacterStream(1, null, 1L));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateCharacterStream("", null, 1));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateCharacterStream("", null, 1L));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateClob(1, c));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateClob(1, rd));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateClob("", c));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateClob("", rd));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateClob(1, null, 1L));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateClob("", null, 1L));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateDate(1, null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateDate("", null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateDouble(1, 0d));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateDouble("", 0d));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateFloat(1, 0f));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateFloat("", 0f));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateInt(1, 0));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateInt("", 0));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateLong(1, 0L));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateLong("", 0L));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateNCharacterStream(1, null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateNCharacterStream("", null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateNCharacterStream(1, null, 1L));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateNCharacterStream("", null, 1L));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateNClob(1, nc));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateNClob(1, rd));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateNClob("", nc));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateNClob("", rd));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateNClob(1, null, 1));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateNClob("", null, 1));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateNString(1, null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateNString("", null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateNull(1));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateNull(""));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateObject(1, null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateObject("", null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateObject(1, null, 1));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateObject("", null, 1));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateRef(1, null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateRef("", null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateRow());
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateRowId(1, null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateRowId("", null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateSQLXML(1, null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateSQLXML("", null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateShort(1, (short) 0));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateShort("", (short) 0));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateString(1, null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateString("", null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateTime(1, null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateTime("", null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateTimestamp(1, null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateTimestamp("", null));
        assertThrowsWritesUnsupportedForUpdate(() -> r.insertRow());
        assertThrowsWritesUnsupportedForUpdate(() -> r.updateRow());
        assertThrowsWritesUnsupportedForUpdate(() -> r.deleteRow());
        assertThrowsWritesUnsupportedForUpdate(() -> r.cancelRowUpdates());
        assertThrowsWritesUnsupportedForUpdate(() -> r.moveToInsertRow());
        assertThrowsWritesUnsupportedForUpdate(() -> r.refreshRow());
        assertThrowsWritesUnsupportedForUpdate(() -> r.moveToCurrentRow());
        assertThrowsWritesUnsupportedForUpdate(() -> r.rowUpdated());
        assertThrowsWritesUnsupportedForUpdate(() -> r.rowInserted());
        assertThrowsWritesUnsupportedForUpdate(() -> r.rowDeleted());
    }
    
    public void testResultSetNotInitialized() throws Exception {
        createTestDataForNumericValueTypes(() -> randomInt());

        SQLException sqle = expectThrows(SQLException.class, () -> {
            doWithQuery(SELECT_WILDCARD, rs -> {
                assertFalse(rs.isAfterLast());
                rs.getObject(1);
            });
        });
        assertEquals("No row available", sqle.getMessage());
    }

    public void testResultSetConsumed() throws Exception {
        createTestDataForNumericValueTypes(() -> randomInt());

        SQLException sqle = expectThrows(SQLException.class, () -> {
            doWithQuery("SELECT * FROM test LIMIT 1", rs -> {
                assertFalse(rs.isAfterLast());
                assertTrue(rs.next());
                assertFalse(rs.isAfterLast());
                assertFalse(rs.next());
                assertTrue(rs.isAfterLast());
                rs.getObject(1);
            });
        });
        assertEquals("No row available", sqle.getMessage());
    }

    private void doWithQuery(String query, CheckedConsumer<ResultSet, SQLException> consumer) throws SQLException {
        doWithQuery(() -> esJdbc(timeZoneId), query, consumer);
    }
    
    private void doWithQueryAndTimezone(String query, String tz, CheckedConsumer<ResultSet, SQLException> consumer) throws SQLException {
        doWithQuery(() -> esJdbc(tz), query, consumer);
    }
    
    private void doWithQuery(CheckedSupplier<Connection, SQLException> con, String query, CheckedConsumer<ResultSet, SQLException> consumer)
            throws SQLException {
        try (Connection connection = con.get()) {
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                try (ResultSet results = statement.executeQuery()) {
                    consumer.accept(results);
                }
            }
        }
    }
    
    protected static void createIndex(String index) throws Exception {
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
            createIndex.startObject("properties");
            {}
            createIndex.endObject();
        }
        createIndex.endObject().endObject();
        request.setJsonEntity(Strings.toString(createIndex));
        client().performRequest(request);
    }

    protected static void updateMapping(String index, CheckedConsumer<XContentBuilder, IOException> body) throws Exception {
        Request request = new Request("PUT", "/" + index + "/_mapping");
        XContentBuilder updateMapping = JsonXContent.contentBuilder().startObject();
        updateMapping.startObject("properties");
        {
            body.accept(updateMapping);
        }
        updateMapping.endObject().endObject();

        request.setJsonEntity(Strings.toString(updateMapping));
        client().performRequest(request);
    }

    private void createTestDataForMultiValueTests() throws Exception {
        createIndex("test");
        updateMapping("test", builder -> {
            builder.startObject("int").field("type", "integer").endObject();
            builder.startObject("keyword").field("type", "keyword").endObject();
        });

        Integer[] values = randomArray(3, 15, s -> new Integer[s], () -> Integer.valueOf(randomInt(50)));
        // add the known value as the first one in list. Parsing from _source the value will pick up the first value in the array.
        values[0] = Integer.valueOf(-10);

        String[] stringValues = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            stringValues[i] = String.valueOf(values[i]);
        }

        index("test", "1", builder -> {
            builder.array("int", (Object[]) values);
            builder.array("keyword", stringValues);
        });
    }
    
    private void createTestDataForMultiValuesInObjectsTests() throws Exception {
        createIndex("test");
        updateMapping("test", builder -> {
            builder.startObject("object")
                .startObject("properties")
                    .startObject("intsubfield").field("type", "integer").endObject()
                    .startObject("textsubfield")
                        .field("type", "text")
                        .startObject("fields").startObject("keyword").field("type", "keyword").endObject().endObject()
                    .endObject()
                .endObject()
            .endObject();
            builder.startObject("keyword").field("type", "keyword").endObject();
        });

        Integer[] values = randomArray(3, 15, s -> new Integer[s], () -> Integer.valueOf(randomInt(50)));
        // add the known value as the first one in list. Parsing from _source the value will pick up the first value in the array.
        values[0] = Integer.valueOf(-25);

        String[] stringValues = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            stringValues[i] = String.valueOf(values[i]);
        }
        stringValues[1] = "xyz";

        index("test", "1", builder -> {
            builder.startArray("object");
                for (int i = 0; i < values.length; i++) {
                    builder.startObject()
                        .field("intsubfield", values[i])
                        .field("textsubfield", stringValues[i])
                    .endObject();
                }
            builder.endArray();
        });
    }

    private void createTestDataForByteValueTests(byte random1, byte random2, byte random3) throws Exception {
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

    private void createTestDataForShortValueTests(short random1, short random2, short random3) throws Exception {
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

    private void createTestDataForIntegerValueTests(int random1, int random2, int random3) throws Exception {
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

    private void createTestDataForLongValueTests(long random1, long random2, long random3) throws Exception {
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

    private void createTestDataForDoubleValueTests(double random1, double random2, double random3) throws Exception {
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

    private void createTestDataForFloatValueTests(float random1, float random2, float random3) throws Exception {
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
    
    private void indexSimpleDocumentWithTrueValues(Long randomLongDate) throws IOException {
        index("test", "1", builder -> {
            builder.field("test_boolean", true);
            builder.field("test_byte", 1);
            builder.field("test_integer", 1);
            builder.field("test_long", 1L);
            builder.field("test_short", 1);
            builder.field("test_double", 1d);
            builder.field("test_float", 1f);
            builder.field("test_keyword", "true");
            builder.field("test_date", randomLongDate);
        });
    }

    /**
     * Creates test data for all numeric get* methods. All values random and different from the other numeric fields already generated.
     * It returns a map containing the field name and its randomly generated value to be later used in checking the returned values.
     */
    private Map<String, Number> createTestDataForNumericValueTypes(Supplier<Number> randomGenerator) throws Exception {
        Map<String,Number> map = new HashMap<>();
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

    private static void updateMappingForNumericValuesTests(String indexName) throws Exception {
        updateMapping(indexName, builder -> {
            for(String field : fieldsNames) {
                builder.startObject(field).field("type", field.substring(5)).endObject();
            }
        });
    }
    
    private void assertThrowsUnsupportedAndExpectErrorMessage(ThrowingRunnable runnable, String message) {
        SQLException sqle = expectThrows(SQLFeatureNotSupportedException.class, runnable);
        assertEquals(message, sqle.getMessage());
    }

    private void assertThrowsWritesUnsupportedForUpdate(ThrowingRunnable r) {
        assertThrowsUnsupportedAndExpectErrorMessage(r, "Writes not supported");
    }
    
    private void validateErrorsForDateTestsWithoutCalendar(CheckedFunction<String,Object,SQLException> method) {
        SQLException sqle;
        for (Entry<Tuple<String, Object>, SQLType> field : dateTimeTestingFields.entrySet()) {
            sqle = expectThrows(SQLException.class, () -> method.apply(field.getKey().v1()));
            assertEquals(
                    format(Locale.ROOT, "Unable to convert value [%.128s] of type [%s] to a Date",
                            field.getKey().v2(), field.getValue()), sqle.getMessage());
        }
    }

    private void validateErrorsForTimeTestsWithoutCalendar(CheckedFunction<String,Object,SQLException> method) {
        SQLException sqle;
        for (Entry<Tuple<String, Object>, SQLType> field : dateTimeTestingFields.entrySet()) {
            sqle = expectThrows(SQLException.class, () -> method.apply(field.getKey().v1()));
            assertEquals(
                format(Locale.ROOT, "Unable to convert value [%.128s] of type [%s] to a Time",
                    field.getKey().v2(), field.getValue()), sqle.getMessage());
        }
    }
    
    private void validateErrorsForDateTimeTestsWithCalendar(Calendar c, CheckedBiFunction<String,Calendar,Object,SQLException> method) {
        SQLException sqle;
        for (Entry<Tuple<String, Object>, SQLType> field : dateTimeTestingFields.entrySet()) {
            sqle = expectThrows(SQLException.class, () -> method.apply(field.getKey().v1(), c));
            assertEquals(
                    format(Locale.ROOT, "Unable to convert value [%.128s] of type [%s] to a Long",
                            field.getKey().v2(), field.getValue()), sqle.getMessage());
        }
    }
    
    private float randomFloatBetween(float start, float end) {
        float result = 0.0f;
        while (result < start || result > end || Float.isNaN(result)) {
            result = start + randomFloat() * (end - start);
        }
        
        return result;
    }

    private Long getMaxIntPlusOne() {
        return Long.valueOf(Integer.MAX_VALUE) + 1L;
    }

    private Double getMaxLongPlusOne() {
        return Double.valueOf(Long.MAX_VALUE) + 1d;
    }

    private Connection esJdbc(String timeZoneId) throws SQLException {
        Properties connectionProperties = connectionProperties();
        connectionProperties.put(JDBC_TIMEZONE, timeZoneId);
        Connection connection = esJdbc(connectionProperties);
        assertNotNull("The timezone should be specified", connectionProperties.getProperty(JDBC_TIMEZONE));
        return connection;
    }

    private Connection esWithLeniency(boolean multiValueLeniency) throws SQLException {
        String property = "field.multi.value.leniency";
        Properties connectionProperties = connectionProperties();
        connectionProperties.setProperty(property, Boolean.toString(multiValueLeniency));
        Connection connection = esJdbc(connectionProperties);
        assertNotNull("The leniency should be specified", connectionProperties.getProperty(property));
        return connection;
    }

    private String asDateString(long millis) {
        return of(millis, timeZoneId);
    }

    private ZoneId getZoneFromOffset(Long randomLongDate) {
        return ZoneId.of(ZoneId.of(timeZoneId).getRules().getOffset(Instant.ofEpochMilli(randomLongDate)).toString());
    }
    
    private Calendar randomCalendar() {
        return Calendar.getInstance(randomTimeZone(), Locale.ROOT);
    }
}
