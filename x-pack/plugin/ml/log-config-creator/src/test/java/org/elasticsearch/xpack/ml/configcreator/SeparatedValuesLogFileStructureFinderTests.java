/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.common.collect.Tuple;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.ml.configcreator.SeparatedValuesLogFileStructureFinder.levenshteinFieldwiseCompareRows;
import static org.elasticsearch.xpack.ml.configcreator.SeparatedValuesLogFileStructureFinder.levenshteinDistance;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class SeparatedValuesLogFileStructureFinderTests extends LogConfigCreatorTestCase {

    private LogFileStructureFinderFactory factory = new CsvLogFileStructureFinderFactory(TEST_TERMINAL);

    public void testCreateConfigsGivenCompleteCsv() throws Exception {
        String sample = "time,message\n" +
            "2018-05-17T13:41:23,hello\n" +
            "2018-05-17T13:41:32,hello again\n";
        assertTrue(factory.canCreateFromSample(sample));
        String charset = randomFrom(POSSIBLE_CHARSETS);
        String timezone = randomFrom(POSSIBLE_TIMEZONES);
        String elasticsearchHost = randomFrom(POSSIBLE_HOSTNAMES);
        String logstashHost = randomFrom(POSSIBLE_HOSTNAMES);
        SeparatedValuesLogFileStructureFinder structure = (SeparatedValuesLogFileStructureFinder) factory.createFromSample(TEST_FILE_NAME,
            TEST_INDEX_NAME, "time_message", elasticsearchHost, logstashHost, timezone, sample, charset, randomHasByteOrderMarker(charset));
        structure.createConfigs();
        assertThat(structure.getFilebeatToLogstashConfig(), containsString("exclude_lines: ['^\"?time\"?,\"?message\"?']\n"));
        assertThat(structure.getFilebeatToLogstashConfig(),
            containsString("multiline.pattern: '^\"?\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}'\n"));
        assertThat(structure.getFilebeatToLogstashConfig(), containsString(logstashHost));
        assertThat(structure.getLogstashFromFilebeatConfig(), containsString("match => [ \"time\", \"ISO8601\" ]\n"));
        assertThat(structure.getLogstashFromFilebeatConfig(), containsString("columns => [ \"time\", \"message\" ]\n"));
        assertThat(structure.getLogstashFromFilebeatConfig(), containsString(elasticsearchHost));
        assertThat(structure.getLogstashFromFileConfig(), containsString("match => [ \"time\", \"ISO8601\" ]\n"));
        assertThat(structure.getLogstashFromFileConfig(), not(containsString("timezone =>")));
        assertThat(structure.getLogstashFromFileConfig(), containsString(elasticsearchHost));
    }

    public void testCreateConfigsGivenCsvWithIncompleteLastRecord() throws Exception {
        String sample = "message,time,count\n" +
            "\"hello\n" +
            "world\",2018-05-17T13:41:23,1\n" +
            "\"hello again\n"; // note that this last record is truncated
        assertTrue(factory.canCreateFromSample(sample));
        String charset = randomFrom(POSSIBLE_CHARSETS);
        String timezone = randomFrom(POSSIBLE_TIMEZONES);
        String elasticsearchHost = randomFrom(POSSIBLE_HOSTNAMES);
        String logstashHost = randomFrom(POSSIBLE_HOSTNAMES);
        SeparatedValuesLogFileStructureFinder structure = (SeparatedValuesLogFileStructureFinder) factory.createFromSample(TEST_FILE_NAME,
            TEST_INDEX_NAME, "message_time", elasticsearchHost, logstashHost, timezone, sample, charset, randomHasByteOrderMarker(charset));
        structure.createConfigs();
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(structure.getFilebeatToLogstashConfig(), not(containsString("encoding:")));
        } else {
            assertThat(structure.getFilebeatToLogstashConfig(), containsString("encoding: '" + charset.toLowerCase(Locale.ROOT) + "'"));
        }
        assertThat(structure.getFilebeatToLogstashConfig(), containsString("exclude_lines: ['^\"?message\"?,\"?time\"?,\"?count\"?']\n"));
        assertThat(structure.getFilebeatToLogstashConfig(),
            containsString("multiline.pattern: '^.*?,\"?\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}'\n"));
        assertThat(structure.getFilebeatToLogstashConfig(), containsString(logstashHost));
        assertThat(structure.getLogstashFromFilebeatConfig(), containsString("match => [ \"time\", \"ISO8601\" ]\n"));
        assertThat(structure.getLogstashFromFilebeatConfig(), containsString("columns => [ \"message\", \"time\", \"count\" ]\n"));
        assertThat(structure.getLogstashFromFilebeatConfig(), containsString(elasticsearchHost));
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(structure.getLogstashFromFileConfig(), not(containsString("charset =>")));
        } else {
            assertThat(structure.getLogstashFromFileConfig(), containsString("charset => \"" + charset + "\""));
        }
        assertThat(structure.getLogstashFromFileConfig(), containsString("match => [ \"time\", \"ISO8601\" ]\n"));
        assertThat(structure.getLogstashFromFileConfig(), not(containsString("timezone =>")));
        assertThat(structure.getLogstashFromFileConfig(), containsString(elasticsearchHost));
    }

    public void testCreateConfigsGivenCsvWithTrailingNulls() throws Exception {
        String sample = "VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID," +
            "store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount," +
            "improvement_surcharge,total_amount,,\n" +
            "2,2016-12-31 15:15:01,2016-12-31 15:15:09,1,.00,1,N,264,264,2,1,0,0.5,0,0,0.3,1.8,,\n" +
            "1,2016-12-01 00:00:01,2016-12-01 00:10:22,1,1.60,1,N,163,143,2,9,0.5,0.5,0,0,0.3,10.3,,\n" +
            "1,2016-12-01 00:00:01,2016-12-01 00:11:01,1,1.40,1,N,164,229,1,9,0.5,0.5,2.05,0,0.3,12.35,,\n";
        assertTrue(factory.canCreateFromSample(sample));
        String charset = randomFrom(POSSIBLE_CHARSETS);
        String timezone = randomFrom(POSSIBLE_TIMEZONES);
        String elasticsearchHost = randomFrom(POSSIBLE_HOSTNAMES);
        String logstashHost = randomFrom(POSSIBLE_HOSTNAMES);
        SeparatedValuesLogFileStructureFinder structure = (SeparatedValuesLogFileStructureFinder) factory.createFromSample(TEST_FILE_NAME,
            TEST_INDEX_NAME, "nyc-taxi", elasticsearchHost, logstashHost, timezone, sample, charset, randomHasByteOrderMarker(charset));
        structure.createConfigs();
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(structure.getFilebeatToLogstashConfig(), not(containsString("encoding:")));
        } else {
            assertThat(structure.getFilebeatToLogstashConfig(), containsString("encoding: '" + charset.toLowerCase(Locale.ROOT) + "'"));
        }
        assertThat(structure.getFilebeatToLogstashConfig(), containsString("exclude_lines: " +
            "['^\"?VendorID\"?,\"?tpep_pickup_datetime\"?,\"?tpep_dropoff_datetime\"?,\"?passenger_count\"?,\"?trip_distance\"?," +
            "\"?RatecodeID\"?,\"?store_and_fwd_flag\"?,\"?PULocationID\"?,\"?DOLocationID\"?,\"?payment_type\"?,\"?fare_amount\"?," +
            "\"?extra\"?,\"?mta_tax\"?,\"?tip_amount\"?,\"?tolls_amount\"?,\"?improvement_surcharge\"?," +
            "\"?total_amount\"?,\"?\"?,\"?\"?']\n"));
        assertThat(structure.getFilebeatToLogstashConfig(),
            containsString("multiline.pattern: '^.*?,.*?,\"?\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}'\n"));
        assertThat(structure.getFilebeatToLogstashConfig(), containsString(logstashHost));
        assertThat(structure.getLogstashFromFilebeatConfig(),
            containsString("match => [ \"tpep_dropoff_datetime\", \"YYYY-MM-dd HH:mm:ss\" ]\n"));
        assertThat(structure.getLogstashFromFilebeatConfig(),
            containsString("columns => [ \"VendorID\", \"tpep_pickup_datetime\", \"tpep_dropoff_datetime\", \"passenger_count\", " +
                "\"trip_distance\", \"RatecodeID\", \"store_and_fwd_flag\", \"PULocationID\", \"DOLocationID\", \"payment_type\", " +
                "\"fare_amount\", \"extra\", \"mta_tax\", \"tip_amount\", \"tolls_amount\", \"improvement_surcharge\", " +
                "\"total_amount\", \"column18\", \"column19\" ]\n"));
        assertThat(structure.getLogstashFromFilebeatConfig(), containsString(elasticsearchHost));
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(structure.getLogstashFromFileConfig(), not(containsString("charset =>")));
        } else {
            assertThat(structure.getLogstashFromFileConfig(), containsString("charset => \"" + charset + "\""));
        }
        assertThat(structure.getLogstashFromFileConfig(),
            containsString("match => [ \"tpep_dropoff_datetime\", \"YYYY-MM-dd HH:mm:ss\" ]\n"));
        if (timezone == null) {
            assertThat(structure.getLogstashFromFileConfig(), not(containsString("timezone =>")));
        } else {
            assertThat(structure.getLogstashFromFileConfig(), containsString("timezone => \"" + timezone + "\"\n"));
        }
        assertThat(structure.getLogstashFromFileConfig(), containsString(elasticsearchHost));
    }

    public void testCreateConfigsGivenCsvWithTrailingNullsExceptHeader() throws Exception {
        String sample = "VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID," +
                "store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount," +
                "improvement_surcharge,total_amount\n" +
            "2,2016-12-31 15:15:01,2016-12-31 15:15:09,1,.00,1,N,264,264,2,1,0,0.5,0,0,0.3,1.8,,\n" +
            "1,2016-12-01 00:00:01,2016-12-01 00:10:22,1,1.60,1,N,163,143,2,9,0.5,0.5,0,0,0.3,10.3,,\n" +
            "1,2016-12-01 00:00:01,2016-12-01 00:11:01,1,1.40,1,N,164,229,1,9,0.5,0.5,2.05,0,0.3,12.35,,\n";
        assertTrue(factory.canCreateFromSample(sample));
        String charset = randomFrom(POSSIBLE_CHARSETS);
        String timezone = randomFrom(POSSIBLE_TIMEZONES);
        String elasticsearchHost = randomFrom(POSSIBLE_HOSTNAMES);
        String logstashHost = randomFrom(POSSIBLE_HOSTNAMES);
        SeparatedValuesLogFileStructureFinder structure = (SeparatedValuesLogFileStructureFinder) factory.createFromSample(TEST_FILE_NAME,
            TEST_INDEX_NAME, "nyc-taxi", elasticsearchHost, logstashHost, timezone, sample, charset, randomHasByteOrderMarker(charset));
        structure.createConfigs();
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(structure.getFilebeatToLogstashConfig(), not(containsString("encoding:")));
        } else {
            assertThat(structure.getFilebeatToLogstashConfig(), containsString("encoding: '" + charset.toLowerCase(Locale.ROOT) + "'"));
        }
        assertThat(structure.getFilebeatToLogstashConfig(), containsString("exclude_lines: " +
            "['^\"?VendorID\"?,\"?tpep_pickup_datetime\"?,\"?tpep_dropoff_datetime\"?,\"?passenger_count\"?,\"?trip_distance\"?," +
            "\"?RatecodeID\"?,\"?store_and_fwd_flag\"?,\"?PULocationID\"?,\"?DOLocationID\"?,\"?payment_type\"?,\"?fare_amount\"?," +
            "\"?extra\"?,\"?mta_tax\"?,\"?tip_amount\"?,\"?tolls_amount\"?,\"?improvement_surcharge\"?,\"?total_amount\"?']\n"));
        assertThat(structure.getFilebeatToLogstashConfig(),
            containsString("multiline.pattern: '^.*?,.*?,\"?\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}'\n"));
        assertThat(structure.getFilebeatToLogstashConfig(), containsString(logstashHost));
        assertThat(structure.getLogstashFromFilebeatConfig(),
            containsString("match => [ \"tpep_dropoff_datetime\", \"YYYY-MM-dd HH:mm:ss\" ]\n"));
        assertThat(structure.getLogstashFromFilebeatConfig(),
            containsString("columns => [ \"VendorID\", \"tpep_pickup_datetime\", \"tpep_dropoff_datetime\", \"passenger_count\", " +
                "\"trip_distance\", \"RatecodeID\", \"store_and_fwd_flag\", \"PULocationID\", \"DOLocationID\", \"payment_type\", " +
                "\"fare_amount\", \"extra\", \"mta_tax\", \"tip_amount\", \"tolls_amount\", \"improvement_surcharge\", " +
                "\"total_amount\" ]\n"));
        assertThat(structure.getLogstashFromFilebeatConfig(), containsString(elasticsearchHost));
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(structure.getLogstashFromFileConfig(), not(containsString("charset =>")));
        } else {
            assertThat(structure.getLogstashFromFileConfig(), containsString("charset => \"" + charset + "\""));
        }
        assertThat(structure.getLogstashFromFileConfig(),
            containsString("match => [ \"tpep_dropoff_datetime\", \"YYYY-MM-dd HH:mm:ss\" ]\n"));
        if (timezone == null) {
            assertThat(structure.getLogstashFromFileConfig(), not(containsString("timezone =>")));
        } else {
            assertThat(structure.getLogstashFromFileConfig(), containsString("timezone => \"" + timezone + "\"\n"));
        }
        assertThat(structure.getLogstashFromFileConfig(), containsString(elasticsearchHost));
    }

    public void testCreateConfigsGivenCsvWithTimeLastColumn() throws Exception {
        String sample = "\"pos_id\",\"trip_id\",\"latitude\",\"longitude\",\"altitude\",\"timestamp\"\n" +
            "\"1\",\"3\",\"4703.7815\",\"1527.4713\",\"359.9\",\"2017-01-19 16:19:04.742113\"\n" +
            "\"2\",\"3\",\"4703.7815\",\"1527.4714\",\"359.9\",\"2017-01-19 16:19:05.741890\"\n";
        assertTrue(factory.canCreateFromSample(sample));
        String charset = randomFrom(POSSIBLE_CHARSETS);
        String timezone = randomFrom(POSSIBLE_TIMEZONES);
        String elasticsearchHost = randomFrom(POSSIBLE_HOSTNAMES);
        String logstashHost = randomFrom(POSSIBLE_HOSTNAMES);
        SeparatedValuesLogFileStructureFinder structure = (SeparatedValuesLogFileStructureFinder) factory.createFromSample(TEST_FILE_NAME,
            TEST_INDEX_NAME, "positions", elasticsearchHost, logstashHost, timezone, sample, charset, randomHasByteOrderMarker(charset));
        structure.createConfigs();
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(structure.getFilebeatToLogstashConfig(), not(containsString("encoding:")));
        } else {
            assertThat(structure.getFilebeatToLogstashConfig(), containsString("encoding: '" + charset.toLowerCase(Locale.ROOT) + "'"));
        }
        assertThat(structure.getFilebeatToLogstashConfig(), containsString("exclude_lines: " +
            "['^\"?pos_id\"?,\"?trip_id\"?,\"?latitude\"?,\"?longitude\"?,\"?altitude\"?,\"?timestamp\"?']\n"));
        assertThat(structure.getFilebeatToLogstashConfig(), not(containsString("multiline.pattern:")));
        assertThat(structure.getFilebeatToLogstashConfig(), containsString(logstashHost));
        assertThat(structure.getLogstashFromFilebeatConfig(),
            containsString("match => [ \"timestamp\", \"YYYY-MM-dd HH:mm:ss.SSSSSS\" ]\n"));
        assertThat(structure.getLogstashFromFilebeatConfig(),
            containsString("columns => [ \"pos_id\", \"trip_id\", \"latitude\", \"longitude\", \"altitude\", \"timestamp\" ]\n"));
        assertThat(structure.getLogstashFromFilebeatConfig(), containsString(elasticsearchHost));
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(structure.getLogstashFromFileConfig(), not(containsString("charset =>")));
        } else {
            assertThat(structure.getLogstashFromFileConfig(), containsString("charset => \"" + charset + "\""));
        }
        assertThat(structure.getLogstashFromFileConfig(), containsString("match => [ \"timestamp\", \"YYYY-MM-dd HH:mm:ss.SSSSSS\" ]\n"));
        if (timezone == null) {
            assertThat(structure.getLogstashFromFileConfig(), not(containsString("timezone =>")));
        } else {
            assertThat(structure.getLogstashFromFileConfig(), containsString("timezone => \"" + timezone + "\"\n"));
        }
        assertThat(structure.getLogstashFromFileConfig(), containsString(elasticsearchHost));
    }

    public void testFindHeaderFromSampleGivenHeaderInSample() throws IOException {
        String withHeader = "time,airline,responsetime,sourcetype\n" +
            "2014-06-23 00:00:00Z,AAL,132.2046,farequote\n" +
            "2014-06-23 00:00:00Z,JZA,990.4628,farequote\n" +
            "2014-06-23 00:00:01Z,JBU,877.5927,farequote\n" +
            "2014-06-23 00:00:01Z,KLM,1355.4812,farequote\n";

        Tuple<Boolean, String[]> header = SeparatedValuesLogFileStructureFinder.findHeaderFromSample(TEST_TERMINAL,
            SeparatedValuesLogFileStructureFinder.readRows(withHeader, CsvPreference.EXCEL_PREFERENCE).v1());

        assertTrue(header.v1());
        assertThat(header.v2(), arrayContaining("time", "airline", "responsetime", "sourcetype"));
    }

    public void testFindHeaderFromSampleGivenHeaderNotInSample() throws IOException {
        String withoutHeader = "2014-06-23 00:00:00Z,AAL,132.2046,farequote\n" +
            "2014-06-23 00:00:00Z,JZA,990.4628,farequote\n" +
            "2014-06-23 00:00:01Z,JBU,877.5927,farequote\n" +
            "2014-06-23 00:00:01Z,KLM,1355.4812,farequote\n";

        Tuple<Boolean, String[]> header = SeparatedValuesLogFileStructureFinder.findHeaderFromSample(TEST_TERMINAL,
            SeparatedValuesLogFileStructureFinder.readRows(withoutHeader, CsvPreference.EXCEL_PREFERENCE).v1());

        assertFalse(header.v1());
        assertThat(header.v2(), arrayContaining("column1", "column2", "column3", "column4"));
    }

    public void testLevenshteinDistance() {

        assertEquals(0, levenshteinDistance("cat", "cat"));
        assertEquals(3, levenshteinDistance("cat", "dog"));
        assertEquals(5, levenshteinDistance("cat", "mouse"));
        assertEquals(3, levenshteinDistance("cat", ""));

        assertEquals(3, levenshteinDistance("dog", "cat"));
        assertEquals(0, levenshteinDistance("dog", "dog"));
        assertEquals(4, levenshteinDistance("dog", "mouse"));
        assertEquals(3, levenshteinDistance("dog", ""));

        assertEquals(5, levenshteinDistance("mouse", "cat"));
        assertEquals(4, levenshteinDistance("mouse", "dog"));
        assertEquals(0, levenshteinDistance("mouse", "mouse"));
        assertEquals(5, levenshteinDistance("mouse", ""));

        assertEquals(3, levenshteinDistance("", "cat"));
        assertEquals(3, levenshteinDistance("", "dog"));
        assertEquals(5, levenshteinDistance("", "mouse"));
        assertEquals(0, levenshteinDistance("", ""));
    }

    public void testLevenshteinCompareRows() {

        assertEquals(0, levenshteinFieldwiseCompareRows(Arrays.asList("cat", "dog"), Arrays.asList("cat", "dog")));
        assertEquals(0, levenshteinFieldwiseCompareRows(Arrays.asList("cat", "dog"), Arrays.asList("cat", "cat")));
        assertEquals(3, levenshteinFieldwiseCompareRows(Arrays.asList("cat", "dog"), Arrays.asList("dog", "cat")));
        assertEquals(3, levenshteinFieldwiseCompareRows(Arrays.asList("cat", "dog"), Arrays.asList("mouse", "cat")));
        assertEquals(5, levenshteinFieldwiseCompareRows(Arrays.asList("cat", "dog", "mouse"), Arrays.asList("mouse", "dog", "cat")));
        assertEquals(4, levenshteinFieldwiseCompareRows(Arrays.asList("cat", "dog", "mouse"), Arrays.asList("mouse", "mouse", "mouse")));
        assertEquals(7, levenshteinFieldwiseCompareRows(Arrays.asList("cat", "dog", "mouse"), Arrays.asList("mouse", "cat", "dog")));
    }

    public void testMakeColumnConversions() {
        Map<String, Object> mappings = new LinkedHashMap<>();
        mappings.put("f1", Collections.singletonMap(AbstractLogFileStructureFinder.MAPPING_TYPE_SETTING, "long"));
        mappings.put("f2", Collections.singletonMap(AbstractLogFileStructureFinder.MAPPING_TYPE_SETTING, "date"));
        mappings.put("f3", Collections.singletonMap(AbstractLogFileStructureFinder.MAPPING_TYPE_SETTING, "text"));
        mappings.put("f4", Collections.singletonMap(AbstractLogFileStructureFinder.MAPPING_TYPE_SETTING, "keyword"));
        mappings.put("f5", Collections.singletonMap(AbstractLogFileStructureFinder.MAPPING_TYPE_SETTING, "double"));
        mappings.put("f6", Collections.singletonMap(AbstractLogFileStructureFinder.MAPPING_TYPE_SETTING, "long"));
        mappings.put("f7", Collections.singletonMap(AbstractLogFileStructureFinder.MAPPING_TYPE_SETTING, "boolean"));
        mappings.put("f8", Collections.singletonMap(AbstractLogFileStructureFinder.MAPPING_TYPE_SETTING, "keyword"));
        String conversions = SeparatedValuesLogFileStructureFinder.makeColumnConversions(mappings);
        assertEquals("    convert => {\n" +
            "      \"f1\" => \"integer\"\n" +
            "      \"f5\" => \"float\"\n" +
            "      \"f6\" => \"integer\"\n" +
            "      \"f7\" => \"boolean\"\n" +
            "    }\n", conversions);
    }

    public void testLineHasUnescapedQuote() {
        assertFalse(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("a,b,c", CsvPreference.EXCEL_PREFERENCE));
        assertFalse(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("\"a\",b,c", CsvPreference.EXCEL_PREFERENCE));
        assertFalse(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("\"a,b\",c", CsvPreference.EXCEL_PREFERENCE));
        assertFalse(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("\"a,b,c\"", CsvPreference.EXCEL_PREFERENCE));
        assertFalse(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("a,\"b\",c", CsvPreference.EXCEL_PREFERENCE));
        assertFalse(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("a,b,\"c\"", CsvPreference.EXCEL_PREFERENCE));
        assertFalse(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("a,\"b\"\"\",c", CsvPreference.EXCEL_PREFERENCE));
        assertFalse(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("a,b,\"c\"\"\"", CsvPreference.EXCEL_PREFERENCE));
        assertFalse(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("\"\"\"a\",b,c", CsvPreference.EXCEL_PREFERENCE));
        assertFalse(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("\"a\"\"\",b,c", CsvPreference.EXCEL_PREFERENCE));
        assertFalse(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("\"a,\"\"b\",c", CsvPreference.EXCEL_PREFERENCE));
        assertTrue(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("between\"words,b,c", CsvPreference.EXCEL_PREFERENCE));
        assertTrue(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("x and \"y\",b,c", CsvPreference.EXCEL_PREFERENCE));

        assertFalse(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("a\tb\tc", CsvPreference.TAB_PREFERENCE));
        assertFalse(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("\"a\"\tb\tc", CsvPreference.TAB_PREFERENCE));
        assertFalse(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("\"a\tb\"\tc", CsvPreference.TAB_PREFERENCE));
        assertFalse(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("\"a\tb\tc\"", CsvPreference.TAB_PREFERENCE));
        assertFalse(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("a\t\"b\"\tc", CsvPreference.TAB_PREFERENCE));
        assertFalse(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("a\tb\t\"c\"", CsvPreference.TAB_PREFERENCE));
        assertFalse(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("a\t\"b\"\"\"\tc", CsvPreference.TAB_PREFERENCE));
        assertFalse(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("a\tb\t\"c\"\"\"", CsvPreference.TAB_PREFERENCE));
        assertFalse(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("\"\"\"a\"\tb\tc", CsvPreference.TAB_PREFERENCE));
        assertFalse(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("\"a\"\"\"\tb\tc", CsvPreference.TAB_PREFERENCE));
        assertFalse(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("\"a\t\"\"b\"\tc", CsvPreference.TAB_PREFERENCE));
        assertTrue(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("between\"words\tb\tc", CsvPreference.TAB_PREFERENCE));
        assertTrue(SeparatedValuesLogFileStructureFinder.lineHasUnescapedQuote("x and \"y\"\tb\tc", CsvPreference.TAB_PREFERENCE));
    }
}
