/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.xpack.ml.filestructurefinder.FileStructureFinder;
import org.elasticsearch.xpack.ml.filestructurefinder.FileStructureFinderManager;
import org.elasticsearch.xpack.ml.filestructurefinder.FileStructureUtils;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class LogConfigWriterTests extends LogConfigCreatorTestCase {

    private FileStructureFinderManager structureFinderManager = new FileStructureFinderManager();

    public void testBestLogstashQuoteFor() {
        assertEquals("\"", LogConfigWriter.bestLogstashQuoteFor("normal"));
        assertEquals("\"", LogConfigWriter.bestLogstashQuoteFor("1"));
        assertEquals("\"", LogConfigWriter.bestLogstashQuoteFor("field with spaces"));
        assertEquals("\"", LogConfigWriter.bestLogstashQuoteFor("field_with_'_in_it"));
        assertEquals("'", LogConfigWriter.bestLogstashQuoteFor("field_with_\"_in_it"));
    }

    public void testMakeColumnConversions() {
        Map<String, Object> mappings = new LinkedHashMap<>();
        mappings.put("f1", Collections.singletonMap(FileStructureUtils.MAPPING_TYPE_SETTING, "long"));
        mappings.put("f2", Collections.singletonMap(FileStructureUtils.MAPPING_TYPE_SETTING, "date"));
        mappings.put("f3", Collections.singletonMap(FileStructureUtils.MAPPING_TYPE_SETTING, "text"));
        mappings.put("f4", Collections.singletonMap(FileStructureUtils.MAPPING_TYPE_SETTING, "keyword"));
        mappings.put("f5", Collections.singletonMap(FileStructureUtils.MAPPING_TYPE_SETTING, "double"));
        mappings.put("f6", Collections.singletonMap(FileStructureUtils.MAPPING_TYPE_SETTING, "long"));
        mappings.put("f7", Collections.singletonMap(FileStructureUtils.MAPPING_TYPE_SETTING, "boolean"));
        mappings.put("f8", Collections.singletonMap(FileStructureUtils.MAPPING_TYPE_SETTING, "keyword"));
        String conversions = LogConfigWriter.makeColumnConversions(mappings);
        assertEquals("    convert => {\n" +
            "      \"f1\" => \"integer\"\n" +
            "      \"f5\" => \"float\"\n" +
            "      \"f6\" => \"integer\"\n" +
            "      \"f7\" => \"boolean\"\n" +
            "    }\n", conversions);
    }

    public void testCreateConfigsGivenGoodJson() throws Exception {
        String charset = randomFrom(REDUCED_CHARSETS);
        String timezone = randomFrom(POSSIBLE_TIMEZONES);
        String elasticsearchHost = randomFrom(POSSIBLE_HOSTNAMES);
        String logstashHost = randomFrom(POSSIBLE_HOSTNAMES);
        FileStructureFinder structureFinder = structureFinderManager.findFileStructure(explanation, Integer.MAX_VALUE,
            new ByteArrayInputStream((randomByteOrderMarker(charset) + JSON_SAMPLE).getBytes(charset)));
        LogConfigWriter logConfigWriter = new LogConfigWriter(TEST_TERMINAL, null, TEST_FILE_NAME, TEST_INDEX_NAME,
            "ml-cpp", elasticsearchHost, logstashHost, timezone);
        logConfigWriter.createConfigs(structureFinder.getStructure(), structureFinder.getSampleMessages());
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(logConfigWriter.getFilebeatToLogstashConfig(), not(containsString("encoding:")));
        } else {
            assertThat(logConfigWriter.getFilebeatToLogstashConfig(),
                containsString("encoding: '" + charset.toLowerCase(Locale.ROOT) + "'"));
        }
        assertThat(logConfigWriter.getFilebeatToLogstashConfig(), containsString(logstashHost));
        assertThat(logConfigWriter.getLogstashFromFilebeatConfig(), containsString("match => [ \"timestamp\", \"UNIX_MS\" ]\n"));
        assertThat(logConfigWriter.getLogstashFromFilebeatConfig(), containsString(elasticsearchHost));
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(logConfigWriter.getLogstashFromFileConfig(), not(containsString("charset =>")));
        } else {
            assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString("charset => \"" + charset + "\""));
        }
        assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString("match => [ \"timestamp\", \"UNIX_MS\" ]\n"));
        assertThat(logConfigWriter.getLogstashFromFileConfig(), not(containsString("timezone =>")));
        assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString(elasticsearchHost));
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(logConfigWriter.getFilebeatToIngestPipelineConfig(), not(containsString("encoding:")));
        } else {
            assertThat(logConfigWriter.getFilebeatToIngestPipelineConfig(),
                containsString("encoding: '" + charset.toLowerCase(Locale.ROOT) + "'"));
        }
        assertThat(logConfigWriter.getFilebeatToIngestPipelineConfig(), containsString(elasticsearchHost));
        assertThat(logConfigWriter.getIngestPipelineFromFilebeatConfig(), containsString("\"field\": \"timestamp\",\n"));
        assertThat(logConfigWriter.getIngestPipelineFromFilebeatConfig(), containsString("\"formats\": [ \"UNIX_MS\" ]\n"));
    }

    public void testCreateConfigsGivenGoodXml() throws Exception {
        String charset = randomFrom(REDUCED_CHARSETS);
        String timezone = randomFrom(POSSIBLE_TIMEZONES);
        String elasticsearchHost = randomFrom(POSSIBLE_HOSTNAMES);
        String logstashHost = randomFrom(POSSIBLE_HOSTNAMES);
        FileStructureFinder structureFinder = structureFinderManager.findFileStructure(explanation, Integer.MAX_VALUE,
            new ByteArrayInputStream((randomByteOrderMarker(charset) + XML_SAMPLE).getBytes(charset)));
        LogConfigWriter logConfigWriter = new LogConfigWriter(TEST_TERMINAL, null, TEST_FILE_NAME, TEST_INDEX_NAME,
            "log4cxx-xml", elasticsearchHost, logstashHost, timezone);
        logConfigWriter.createConfigs(structureFinder.getStructure(), structureFinder.getSampleMessages());
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(logConfigWriter.getFilebeatToLogstashConfig(), not(containsString("encoding:")));
        } else {
            assertThat(logConfigWriter.getFilebeatToLogstashConfig(),
                containsString("encoding: '" + charset.toLowerCase(Locale.ROOT) + "'"));
        }
        assertThat(logConfigWriter.getFilebeatToLogstashConfig(), containsString("multiline.pattern: '^\\s*<log4j:event'\n"));
        assertThat(logConfigWriter.getFilebeatToLogstashConfig(), containsString(logstashHost));
        assertThat(logConfigWriter.getLogstashFromFilebeatConfig(),
            containsString("match => [ \"[log4j:event][timestamp]\", \"UNIX_MS\" ]\n"));
        assertThat(logConfigWriter.getLogstashFromFilebeatConfig(), containsString(elasticsearchHost));
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(logConfigWriter.getLogstashFromFileConfig(), not(containsString("charset =>")));
        } else {
            assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString("charset => \"" + charset + "\""));
        }
        assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString("pattern => \"^\\s*<log4j:event\"\n"));
        assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString("match => [ \"[log4j:event][timestamp]\", \"UNIX_MS\" ]\n"));
        assertThat(logConfigWriter.getLogstashFromFileConfig(), not(containsString("timezone =>")));
        assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString(elasticsearchHost));
    }

    public void testCreateConfigsGivenCompleteCsv() throws Exception {
        String sample = "time,message\n" +
            "2018-05-17T13:41:23,hello\n" +
            "2018-05-17T13:41:32,hello again\n";
        String charset = randomFrom(REDUCED_CHARSETS);
        String timezone = randomFrom(POSSIBLE_TIMEZONES);
        String elasticsearchHost = randomFrom(POSSIBLE_HOSTNAMES);
        String logstashHost = randomFrom(POSSIBLE_HOSTNAMES);
        FileStructureFinder structureFinder = structureFinderManager.findFileStructure(explanation, Integer.MAX_VALUE,
            new ByteArrayInputStream((randomByteOrderMarker(charset) + sample).getBytes(charset)));
        LogConfigWriter logConfigWriter = new LogConfigWriter(TEST_TERMINAL, null, TEST_FILE_NAME, TEST_INDEX_NAME,
            "message_time", elasticsearchHost, logstashHost, timezone);
        logConfigWriter.createConfigs(structureFinder.getStructure(), structureFinder.getSampleMessages());
        assertThat(logConfigWriter.getFilebeatToLogstashConfig(), containsString("exclude_lines: ['^\"?time\"?,\"?message\"?']\n"));
        assertThat(logConfigWriter.getFilebeatToLogstashConfig(),
            containsString("multiline.pattern: '^\"?\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}'\n"));
        assertThat(logConfigWriter.getFilebeatToLogstashConfig(), containsString(logstashHost));
        assertThat(logConfigWriter.getLogstashFromFilebeatConfig(), containsString("match => [ \"time\", \"ISO8601\" ]\n"));
        assertThat(logConfigWriter.getLogstashFromFilebeatConfig(), containsString("columns => [ \"time\", \"message\" ]\n"));
        assertThat(logConfigWriter.getLogstashFromFilebeatConfig(), containsString(elasticsearchHost));
        assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString("match => [ \"time\", \"ISO8601\" ]\n"));
        assertThat(logConfigWriter.getLogstashFromFileConfig(), not(containsString("timezone =>")));
        assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString(elasticsearchHost));
    }

    public void testCreateConfigsGivenCsvWithIncompleteLastRecord() throws Exception {
        String sample = "message,time,count\n" +
            "\"hello\n" +
            "world\",2018-05-17T13:41:23,1\n" +
            "\"hello again\n"; // note that this last record is truncated
        String charset = randomFrom(REDUCED_CHARSETS);
        String timezone = randomFrom(POSSIBLE_TIMEZONES);
        String elasticsearchHost = randomFrom(POSSIBLE_HOSTNAMES);
        String logstashHost = randomFrom(POSSIBLE_HOSTNAMES);
        FileStructureFinder structureFinder = structureFinderManager.findFileStructure(explanation, Integer.MAX_VALUE,
            new ByteArrayInputStream((randomByteOrderMarker(charset) + sample).getBytes(charset)));
        LogConfigWriter logConfigWriter = new LogConfigWriter(TEST_TERMINAL, null, TEST_FILE_NAME, TEST_INDEX_NAME,
            "message_time", elasticsearchHost, logstashHost, timezone);
        logConfigWriter.createConfigs(structureFinder.getStructure(), structureFinder.getSampleMessages());
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(logConfigWriter.getFilebeatToLogstashConfig(), not(containsString("encoding:")));
        } else {
            assertThat(logConfigWriter.getFilebeatToLogstashConfig(),
                containsString("encoding: '" + charset.toLowerCase(Locale.ROOT) + "'"));
        }
        assertThat(logConfigWriter.getFilebeatToLogstashConfig(),
            containsString("exclude_lines: ['^\"?message\"?,\"?time\"?,\"?count\"?']\n"));
        assertThat(logConfigWriter.getFilebeatToLogstashConfig(),
            containsString("multiline.pattern: '^.*?,\"?\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}'\n"));
        assertThat(logConfigWriter.getFilebeatToLogstashConfig(), containsString(logstashHost));
        assertThat(logConfigWriter.getLogstashFromFilebeatConfig(), containsString("match => [ \"time\", \"ISO8601\" ]\n"));
        assertThat(logConfigWriter.getLogstashFromFilebeatConfig(), containsString("columns => [ \"message\", \"time\", \"count\" ]\n"));
        assertThat(logConfigWriter.getLogstashFromFilebeatConfig(), containsString(elasticsearchHost));
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(logConfigWriter.getLogstashFromFileConfig(), not(containsString("charset =>")));
        } else {
            assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString("charset => \"" + charset + "\""));
        }
        assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString("match => [ \"time\", \"ISO8601\" ]\n"));
        assertThat(logConfigWriter.getLogstashFromFileConfig(), not(containsString("timezone =>")));
        assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString(elasticsearchHost));
    }

    public void testCreateConfigsGivenCsvWithTrailingNulls() throws Exception {
        String sample = "VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID," +
            "store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount," +
            "improvement_surcharge,total_amount,,\n" +
            "2,2016-12-31 15:15:01,2016-12-31 15:15:09,1,.00,1,N,264,264,2,1,0,0.5,0,0,0.3,1.8,,\n" +
            "1,2016-12-01 00:00:01,2016-12-01 00:10:22,1,1.60,1,N,163,143,2,9,0.5,0.5,0,0,0.3,10.3,,\n" +
            "1,2016-12-01 00:00:01,2016-12-01 00:11:01,1,1.40,1,N,164,229,1,9,0.5,0.5,2.05,0,0.3,12.35,,\n";
        String charset = randomFrom(REDUCED_CHARSETS);
        String timezone = randomFrom(POSSIBLE_TIMEZONES);
        String elasticsearchHost = randomFrom(POSSIBLE_HOSTNAMES);
        String logstashHost = randomFrom(POSSIBLE_HOSTNAMES);
        FileStructureFinder structureFinder = structureFinderManager.findFileStructure(explanation, Integer.MAX_VALUE,
            new ByteArrayInputStream((randomByteOrderMarker(charset) + sample).getBytes(charset)));
        LogConfigWriter logConfigWriter = new LogConfigWriter(TEST_TERMINAL, null, TEST_FILE_NAME, TEST_INDEX_NAME,
            "nyc-taxi", elasticsearchHost, logstashHost, timezone);
        logConfigWriter.createConfigs(structureFinder.getStructure(), structureFinder.getSampleMessages());
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(logConfigWriter.getFilebeatToLogstashConfig(), not(containsString("encoding:")));
        } else {
            assertThat(logConfigWriter.getFilebeatToLogstashConfig(),
                containsString("encoding: '" + charset.toLowerCase(Locale.ROOT) + "'"));
        }
        assertThat(logConfigWriter.getFilebeatToLogstashConfig(), containsString("exclude_lines: " +
            "['^\"?VendorID\"?,\"?tpep_pickup_datetime\"?,\"?tpep_dropoff_datetime\"?,\"?passenger_count\"?,\"?trip_distance\"?," +
            "\"?RatecodeID\"?,\"?store_and_fwd_flag\"?,\"?PULocationID\"?,\"?DOLocationID\"?,\"?payment_type\"?,\"?fare_amount\"?," +
            "\"?extra\"?,\"?mta_tax\"?,\"?tip_amount\"?,\"?tolls_amount\"?,\"?improvement_surcharge\"?," +
            "\"?total_amount\"?,\"?\"?,\"?\"?']\n"));
        assertThat(logConfigWriter.getFilebeatToLogstashConfig(),
            containsString("multiline.pattern: '^.*?,\"?\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}'\n"));
        assertThat(logConfigWriter.getFilebeatToLogstashConfig(), containsString(logstashHost));
        assertThat(logConfigWriter.getLogstashFromFilebeatConfig(),
            containsString("match => [ \"tpep_pickup_datetime\", \"YYYY-MM-dd HH:mm:ss\" ]\n"));
        assertThat(logConfigWriter.getLogstashFromFilebeatConfig(),
            containsString("columns => [ \"VendorID\", \"tpep_pickup_datetime\", \"tpep_dropoff_datetime\", \"passenger_count\", " +
                "\"trip_distance\", \"RatecodeID\", \"store_and_fwd_flag\", \"PULocationID\", \"DOLocationID\", \"payment_type\", " +
                "\"fare_amount\", \"extra\", \"mta_tax\", \"tip_amount\", \"tolls_amount\", \"improvement_surcharge\", " +
                "\"total_amount\", \"column18\", \"column19\" ]\n"));
        assertThat(logConfigWriter.getLogstashFromFilebeatConfig(), containsString(elasticsearchHost));
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(logConfigWriter.getLogstashFromFileConfig(), not(containsString("charset =>")));
        } else {
            assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString("charset => \"" + charset + "\""));
        }
        assertThat(logConfigWriter.getLogstashFromFileConfig(),
            containsString("match => [ \"tpep_pickup_datetime\", \"YYYY-MM-dd HH:mm:ss\" ]\n"));
        if (timezone == null) {
            assertThat(logConfigWriter.getLogstashFromFileConfig(), not(containsString("timezone =>")));
        } else {
            assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString("timezone => \"" + timezone + "\"\n"));
        }
        assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString(elasticsearchHost));
    }

    public void testCreateConfigsGivenCsvWithTrailingNullsExceptHeader() throws Exception {
        String sample = "VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID," +
            "store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount," +
            "improvement_surcharge,total_amount\n" +
            "2,2016-12-31 15:15:01,2016-12-31 15:15:09,1,.00,1,N,264,264,2,1,0,0.5,0,0,0.3,1.8,,\n" +
            "1,2016-12-01 00:00:01,2016-12-01 00:10:22,1,1.60,1,N,163,143,2,9,0.5,0.5,0,0,0.3,10.3,,\n" +
            "1,2016-12-01 00:00:01,2016-12-01 00:11:01,1,1.40,1,N,164,229,1,9,0.5,0.5,2.05,0,0.3,12.35,,\n";
        String charset = randomFrom(REDUCED_CHARSETS);
        String timezone = randomFrom(POSSIBLE_TIMEZONES);
        String elasticsearchHost = randomFrom(POSSIBLE_HOSTNAMES);
        String logstashHost = randomFrom(POSSIBLE_HOSTNAMES);
        FileStructureFinder structureFinder = structureFinderManager.findFileStructure(explanation, Integer.MAX_VALUE,
            new ByteArrayInputStream((randomByteOrderMarker(charset) + sample).getBytes(charset)));
        LogConfigWriter logConfigWriter = new LogConfigWriter(TEST_TERMINAL, null, TEST_FILE_NAME, TEST_INDEX_NAME,
            "nyc-taxi", elasticsearchHost, logstashHost, timezone);
        logConfigWriter.createConfigs(structureFinder.getStructure(), structureFinder.getSampleMessages());
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(logConfigWriter.getFilebeatToLogstashConfig(), not(containsString("encoding:")));
        } else {
            assertThat(logConfigWriter.getFilebeatToLogstashConfig(),
                containsString("encoding: '" + charset.toLowerCase(Locale.ROOT) + "'"));
        }
        assertThat(logConfigWriter.getFilebeatToLogstashConfig(), containsString("exclude_lines: " +
            "['^\"?VendorID\"?,\"?tpep_pickup_datetime\"?,\"?tpep_dropoff_datetime\"?,\"?passenger_count\"?,\"?trip_distance\"?," +
            "\"?RatecodeID\"?,\"?store_and_fwd_flag\"?,\"?PULocationID\"?,\"?DOLocationID\"?,\"?payment_type\"?,\"?fare_amount\"?," +
            "\"?extra\"?,\"?mta_tax\"?,\"?tip_amount\"?,\"?tolls_amount\"?,\"?improvement_surcharge\"?,\"?total_amount\"?']\n"));
        assertThat(logConfigWriter.getFilebeatToLogstashConfig(),
            containsString("multiline.pattern: '^.*?,\"?\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}'\n"));
        assertThat(logConfigWriter.getFilebeatToLogstashConfig(), containsString(logstashHost));
        assertThat(logConfigWriter.getLogstashFromFilebeatConfig(),
            containsString("match => [ \"tpep_pickup_datetime\", \"YYYY-MM-dd HH:mm:ss\" ]\n"));
        assertThat(logConfigWriter.getLogstashFromFilebeatConfig(),
            containsString("columns => [ \"VendorID\", \"tpep_pickup_datetime\", \"tpep_dropoff_datetime\", \"passenger_count\", " +
                "\"trip_distance\", \"RatecodeID\", \"store_and_fwd_flag\", \"PULocationID\", \"DOLocationID\", \"payment_type\", " +
                "\"fare_amount\", \"extra\", \"mta_tax\", \"tip_amount\", \"tolls_amount\", \"improvement_surcharge\", " +
                "\"total_amount\" ]\n"));
        assertThat(logConfigWriter.getLogstashFromFilebeatConfig(), containsString(elasticsearchHost));
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(logConfigWriter.getLogstashFromFileConfig(), not(containsString("charset =>")));
        } else {
            assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString("charset => \"" + charset + "\""));
        }
        assertThat(logConfigWriter.getLogstashFromFileConfig(),
            containsString("match => [ \"tpep_pickup_datetime\", \"YYYY-MM-dd HH:mm:ss\" ]\n"));
        if (timezone == null) {
            assertThat(logConfigWriter.getLogstashFromFileConfig(), not(containsString("timezone =>")));
        } else {
            assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString("timezone => \"" + timezone + "\"\n"));
        }
        assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString(elasticsearchHost));
    }

    public void testCreateConfigsGivenCsvWithTimeLastColumn() throws Exception {
        String sample = "\"pos_id\",\"trip_id\",\"latitude\",\"longitude\",\"altitude\",\"timestamp\"\n" +
            "\"1\",\"3\",\"4703.7815\",\"1527.4713\",\"359.9\",\"2017-01-19 16:19:04.742113\"\n" +
            "\"2\",\"3\",\"4703.7815\",\"1527.4714\",\"359.9\",\"2017-01-19 16:19:05.741890\"\n";
        String charset = randomFrom(REDUCED_CHARSETS);
        String timezone = randomFrom(POSSIBLE_TIMEZONES);
        String elasticsearchHost = randomFrom(POSSIBLE_HOSTNAMES);
        String logstashHost = randomFrom(POSSIBLE_HOSTNAMES);
        FileStructureFinder structureFinder = structureFinderManager.findFileStructure(explanation, Integer.MAX_VALUE,
            new ByteArrayInputStream((randomByteOrderMarker(charset) + sample).getBytes(charset)));
        LogConfigWriter logConfigWriter = new LogConfigWriter(TEST_TERMINAL, null, TEST_FILE_NAME, TEST_INDEX_NAME,
            "positions", elasticsearchHost, logstashHost, timezone);
        logConfigWriter.createConfigs(structureFinder.getStructure(), structureFinder.getSampleMessages());
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(logConfigWriter.getFilebeatToLogstashConfig(), not(containsString("encoding:")));
        } else {
            assertThat(logConfigWriter.getFilebeatToLogstashConfig(),
                containsString("encoding: '" + charset.toLowerCase(Locale.ROOT) + "'"));
        }
        assertThat(logConfigWriter.getFilebeatToLogstashConfig(), containsString("exclude_lines: " +
            "['^\"?pos_id\"?,\"?trip_id\"?,\"?latitude\"?,\"?longitude\"?,\"?altitude\"?,\"?timestamp\"?']\n"));
        assertThat(logConfigWriter.getFilebeatToLogstashConfig(), not(containsString("multiline.pattern:")));
        assertThat(logConfigWriter.getFilebeatToLogstashConfig(), containsString(logstashHost));
        assertThat(logConfigWriter.getLogstashFromFilebeatConfig(),
            containsString("match => [ \"timestamp\", \"YYYY-MM-dd HH:mm:ss.SSSSSS\" ]\n"));
        assertThat(logConfigWriter.getLogstashFromFilebeatConfig(),
            containsString("columns => [ \"pos_id\", \"trip_id\", \"latitude\", \"longitude\", \"altitude\", \"timestamp\" ]\n"));
        assertThat(logConfigWriter.getLogstashFromFilebeatConfig(), containsString(elasticsearchHost));
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(logConfigWriter.getLogstashFromFileConfig(), not(containsString("charset =>")));
        } else {
            assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString("charset => \"" + charset + "\""));
        }
        assertThat(logConfigWriter.getLogstashFromFileConfig(),
            containsString("match => [ \"timestamp\", \"YYYY-MM-dd HH:mm:ss.SSSSSS\" ]\n"));
        if (timezone == null) {
            assertThat(logConfigWriter.getLogstashFromFileConfig(), not(containsString("timezone =>")));
        } else {
            assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString("timezone => \"" + timezone + "\"\n"));
        }
        assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString(elasticsearchHost));
    }

    public void testCreateConfigsGivenElasticsearchLog() throws Exception {
        String charset = randomFrom(REDUCED_CHARSETS);
        String timezone = randomFrom(POSSIBLE_TIMEZONES);
        String elasticsearchHost = randomFrom(POSSIBLE_HOSTNAMES);
        String logstashHost = randomFrom(POSSIBLE_HOSTNAMES);
        FileStructureFinder structureFinder = structureFinderManager.findFileStructure(explanation, Integer.MAX_VALUE,
            new ByteArrayInputStream((randomByteOrderMarker(charset) + TEXT_SAMPLE).getBytes(charset)));
        LogConfigWriter logConfigWriter = new LogConfigWriter(TEST_TERMINAL, null, TEST_FILE_NAME, TEST_INDEX_NAME,
            "es", elasticsearchHost, logstashHost, timezone);
        logConfigWriter.createConfigs(structureFinder.getStructure(), structureFinder.getSampleMessages());
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(logConfigWriter.getFilebeatToLogstashConfig(), not(containsString("encoding:")));
        } else {
            assertThat(logConfigWriter.getFilebeatToLogstashConfig(),
                containsString("encoding: '" + charset.toLowerCase(Locale.ROOT) + "'"));
        }
        assertThat(logConfigWriter.getFilebeatToLogstashConfig(),
            containsString("multiline.pattern: '^\\[\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2},\\d{3}'\n"));
        assertThat(logConfigWriter.getFilebeatToLogstashConfig(), containsString(logstashHost));
        assertThat(logConfigWriter.getLogstashFromFilebeatConfig(),
            containsString("match => { \"message\" => \"\\[%{TIMESTAMP_ISO8601:timestamp}\\]\\[%{LOGLEVEL:loglevel} \\]" +
                "\\[.*\" }\n"));
        assertThat(logConfigWriter.getLogstashFromFilebeatConfig(), containsString("match => [ \"timestamp\", \"ISO8601\" ]\n"));
        assertThat(logConfigWriter.getLogstashFromFilebeatConfig(), containsString(elasticsearchHost));
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(logConfigWriter.getLogstashFromFileConfig(), not(containsString("charset =>")));
        } else {
            assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString("charset => \"" + charset + "\""));
        }
        assertThat(logConfigWriter.getLogstashFromFileConfig(),
            containsString("match => { \"message\" => \"\\[%{TIMESTAMP_ISO8601:timestamp}\\]\\[%{LOGLEVEL:loglevel} \\]" +
                "\\[.*\" }\n"));
        assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString("match => [ \"timestamp\", \"ISO8601\" ]\n"));
        if (timezone == null) {
            assertThat(logConfigWriter.getLogstashFromFileConfig(), not(containsString("timezone =>")));
        } else {
            assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString("timezone => \"" + timezone + "\"\n"));
        }
        assertThat(logConfigWriter.getLogstashFromFileConfig(), containsString(elasticsearchHost));
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(logConfigWriter.getFilebeatToIngestPipelineConfig(), not(containsString("encoding:")));
        } else {
            assertThat(logConfigWriter.getFilebeatToIngestPipelineConfig(),
                containsString("encoding: '" + charset.toLowerCase(Locale.ROOT) + "'"));
        }
        assertThat(logConfigWriter.getFilebeatToIngestPipelineConfig(),
            containsString("multiline.pattern: '^\\[\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2},\\d{3}'\n"));
        assertThat(logConfigWriter.getFilebeatToIngestPipelineConfig(), containsString(elasticsearchHost));
        assertThat(logConfigWriter.getIngestPipelineFromFilebeatConfig(),
            containsString("\"patterns\": [ \"\\\\[%{TIMESTAMP_ISO8601:timestamp}\\\\]\\\\[%{LOGLEVEL:loglevel} \\\\]" +
                "\\\\[.*\" ]\n"));
        assertThat(logConfigWriter.getIngestPipelineFromFilebeatConfig(), containsString("\"field\": \"timestamp\",\n"));
        assertThat(logConfigWriter.getIngestPipelineFromFilebeatConfig(), containsString("\"formats\": [ \"ISO8601\" ]\n"));
    }
}
