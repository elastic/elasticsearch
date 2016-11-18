/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.process.autodetect.writer;

import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;

public class AggregatedJsonRecordReaderTests extends ESTestCase {
    public void testRead_WithNoTermField() throws IOException {
        String data = "{" + "\"took\" : 88," + "\"timed_out\" : false,"
                + "\"_shards\" : { \"total\" : 5, \"successful\" : 5, \"failed\" : 0 },"
                + "\"hits\" : { \"total\" : 86275, \"max_score\" : 0.0, \"hits\" : [ ] }," + "\"aggregations\" : {" + "\"time_level\" : {"
                + "\"buckets\" : [ {" + "\"key_as_string\" : \"2015-12-07T00:00:00.000Z\", \"key\" : 1449446400000, \"doc_count\" : 649,"
                + "\"metric_level\" : { \"value\" : 106.72129514140468 }" + "}," + "{"
                + "\"key_as_string\" : \"2015-12-07T01:00:00.000Z\", \"key\" : 1449450000000, \"doc_count\" : 627,"
                + "\"metric_level\" : { \"value\" : 103.64676252462097 }" + "} ]" + "}" + "}" + "}";
        JsonParser parser = createParser(data);
        Map<String, Integer> fieldMap = createFieldMapWithNoTermField();
        List<String> nestingOrder = createNestingOrderWithNoTermField();

        AggregatedJsonRecordReader reader = new AggregatedJsonRecordReader(parser, fieldMap, "aggregations", mock(Logger.class),
                nestingOrder);

        String[] record = new String[4];
        boolean[] gotFields = new boolean[4];

        assertEquals(3, reader.read(record, gotFields));
        assertEquals("649", record[0]);
        assertEquals("106.72129514140468", record[1]);
        assertEquals("1449446400000", record[2]);

        assertEquals(3, reader.read(record, gotFields));
        assertEquals("627", record[0]);
        assertEquals("103.64676252462097", record[1]);
        assertEquals("1449450000000", record[2]);

        assertEquals(-1, reader.read(record, gotFields));
    }

    public void testRead_WithOneTermField() throws JsonParseException, IOException {
        String data = "{" + "\"took\" : 88," + "\"timed_out\" : false,"
                + "\"_shards\" : { \"total\" : 5, \"successful\" : 5, \"failed\" : 0 },"
                + "\"hits\" : { \"total\" : 86275, \"max_score\" : 0.0, \"hits\" : [ ] }," + "\"aggregations\" : {" + "\"time_level\" : {"
                + "\"buckets\" : [ {" + "\"key_as_string\" : \"2015-12-07T00:00:00.000Z\", \"key\" : 1449446400000, \"doc_count\" : 649,"
                + "\"airline_level\" : {" + "\"doc_count_error_upper_bound\" : 0, \"sum_other_doc_count\" : 0,"
                + "\"buckets\" : [ { \"key\" : \"aal\", \"doc_count\" : 62, \"metric_level\" : { \"value\" : 106.72129514140468 } },"
                + "{ \"key\" : \"awe\", \"doc_count\" : 61, \"metric_level\" : { \"value\" : 20.20497368984535 } } ]" + "}" + "}," + "{"
                + "\"key_as_string\" : \"2015-12-07T01:00:00.000Z\", \"key\" : 1449450000000, \"doc_count\" : 627,"
                + "\"airline_level\" : {" + "\"doc_count_error_upper_bound\" : 0, \"sum_other_doc_count\" : 0,"
                + "\"buckets\" : [ { \"key\" : \"aal\", \"doc_count\" : 59, \"metric_level\" : { \"value\" : 103.64676252462097 } },"
                + "{ \"key\" : \"awe\", \"doc_count\" : 56, \"metric_level\" : { \"value\" : 20.047162464686803 } } ]" + "}" + "} ]" + "}"
                + "}" + "}";
        JsonParser parser = createParser(data);
        Map<String, Integer> fieldMap = createFieldMapWithOneTermField();
        List<String> nestingOrder = createNestingOrderWithOneTermField();

        AggregatedJsonRecordReader reader = new AggregatedJsonRecordReader(parser, fieldMap, "aggregations", mock(Logger.class),
                nestingOrder);

        String[] record = new String[4];
        boolean[] gotFields = new boolean[4];

        assertEquals(4, reader.read(record, gotFields));
        assertEquals("aal", record[0]);
        assertEquals("62", record[1]);
        assertEquals("106.72129514140468", record[2]);
        assertEquals("1449446400000", record[3]);

        assertEquals(4, reader.read(record, gotFields));
        assertEquals("awe", record[0]);
        assertEquals("61", record[1]);
        assertEquals("20.20497368984535", record[2]);
        assertEquals("1449446400000", record[3]);

        assertEquals(4, reader.read(record, gotFields));
        assertEquals("aal", record[0]);
        assertEquals("59", record[1]);
        assertEquals("103.64676252462097", record[2]);
        assertEquals("1449450000000", record[3]);

        assertEquals(4, reader.read(record, gotFields));
        assertEquals("awe", record[0]);
        assertEquals("56", record[1]);
        assertEquals("20.047162464686803", record[2]);
        assertEquals("1449450000000", record[3]);

        assertEquals(-1, reader.read(record, gotFields));
    }

    public void testRead_WithTwoTermFields() throws JsonParseException, IOException {
        String data = "{" + "\"took\" : 88," + "\"timed_out\" : false,"
                + "\"_shards\" : { \"total\" : 5, \"successful\" : 5, \"failed\" : 0 },"
                + "\"hits\" : { \"total\" : 86275, \"max_score\" : 0.0, \"hits\" : [ ] }," + "\"aggregations\" : {" + "\"time_level\" : {"
                + "\"buckets\" : [ {" + "\"key_as_string\" : \"2015-12-07T00:00:00.000Z\", \"key\" : 1449446400000, \"doc_count\" : 649,"
                + "\"sourcetype_level\" : {" + "\"doc_count_error_upper_bound\" : 0, \"sum_other_doc_count\" : 0," + "\"buckets\" : [ {"
                + "\"key\" : \"farequote\", \"doc_count\" : 649," + "\"airline_level\" : {"
                + "\"doc_count_error_upper_bound\" : 0, \"sum_other_doc_count\" : 0,"
                + "\"buckets\" : [ { \"key\" : \"aal\", \"doc_count\" : 62, \"metric_level\" : { \"value\" : 106.72129514140468 } },"
                + "{ \"key\" : \"awe\", \"doc_count\" : 61, \"metric_level\" : { \"value\" : 20.20497368984535 } } ]" + "}" + "} ]" + "}"
                + "}," + "{" + "\"key_as_string\" : \"2015-12-07T01:00:00.000Z\", \"key\" : 1449450000000, \"doc_count\" : 627,"
                + "\"sourcetype_level\" : {" + "\"doc_count_error_upper_bound\" : 0, \"sum_other_doc_count\" : 0," + "\"buckets\" : [ {"
                + "\"key\" : \"farequote\", \"doc_count\" : 627," + "\"airline_level\" : {"
                + "\"doc_count_error_upper_bound\" : 0, \"sum_other_doc_count\" : 0,"
                + "\"buckets\" : [ { \"key\" : \"aal\", \"doc_count\" : 59, \"metric_level\" : { \"value\" : 103.64676252462097 } },"
                + "{ \"key\" : \"awe\", \"doc_count\" : 56, \"metric_level\" : { \"value\" : 20.047162464686803 } } ]" + "}" + "} ]" + "}"
                + "} ]" + "}" + "}" + "}";
        JsonParser parser = createParser(data);
        Map<String, Integer> fieldMap = createFieldMapWithTwoTermFields();
        List<String> nestingOrder = createNestingOrderWithTwoTermFields();

        AggregatedJsonRecordReader reader = new AggregatedJsonRecordReader(parser, fieldMap, "aggregations", mock(Logger.class),
                nestingOrder);

        String[] record = new String[5];
        boolean[] gotFields = new boolean[5];

        assertEquals(5, reader.read(record, gotFields));
        assertEquals("aal", record[0]);
        assertEquals("62", record[1]);
        assertEquals("106.72129514140468", record[2]);
        assertEquals("1449446400000", record[3]);
        assertEquals("farequote", record[4]);

        assertEquals(5, reader.read(record, gotFields));
        assertEquals("awe", record[0]);
        assertEquals("61", record[1]);
        assertEquals("20.20497368984535", record[2]);
        assertEquals("1449446400000", record[3]);
        assertEquals("farequote", record[4]);

        assertEquals(5, reader.read(record, gotFields));
        assertEquals("aal", record[0]);
        assertEquals("59", record[1]);
        assertEquals("103.64676252462097", record[2]);
        assertEquals("1449450000000", record[3]);
        assertEquals("farequote", record[4]);

        assertEquals(5, reader.read(record, gotFields));
        assertEquals("awe", record[0]);
        assertEquals("56", record[1]);
        assertEquals("20.047162464686803", record[2]);
        assertEquals("1449450000000", record[3]);
        assertEquals("farequote", record[4]);

        assertEquals(-1, reader.read(record, gotFields));
    }

    public void testConstructor_GivenNoNestingOrder() throws JsonParseException, IOException {
        JsonParser parser = createParser("");
        Map<String, Integer> fieldMap = createFieldMapWithNoTermField();
        List<String> nestingOrder = Collections.emptyList();

        ESTestCase.expectThrows(IllegalArgumentException.class,
                () -> new AggregatedJsonRecordReader(parser, fieldMap, "aggregations", mock(Logger.class), nestingOrder));
    }

    public void testRead_GivenInvalidJson() throws JsonParseException, IOException {
        String data = "{" + "\"took\" : 88," + "\"timed_out\" : false,"
                + "\"_shards\" : { \"total\" : 5, \"successful\" : 5, \"failed\" : 0 },"
                + "\"hits\" : { \"total\" : 86275, \"max_score\" : 0.0, \"hits\" : [ ] }," + "\"aggregations\" : {" + "\"time_level\" : {";
        JsonParser parser = createParser(data);
        Map<String, Integer> fieldMap = createFieldMapWithNoTermField();
        List<String> nestingOrder = createNestingOrderWithNoTermField();

        AggregatedJsonRecordReader reader = new AggregatedJsonRecordReader(parser, fieldMap, "aggregations", mock(Logger.class),
                nestingOrder);

        String[] record = new String[4];
        boolean[] gotFields = new boolean[4];

        ESTestCase.expectThrows(ElasticsearchParseException.class, () -> reader.read(record, gotFields));
    }

    private JsonParser createParser(String input) throws JsonParseException, IOException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        return new JsonFactory().createParser(inputStream);
    }

    private Map<String, Integer> createFieldMapWithNoTermField() {
        Map<String, Integer> fieldMap = new HashMap<>();
        fieldMap.put("doc_count", 0);
        fieldMap.put("responsetime", 1);
        fieldMap.put("time", 2);
        return fieldMap;
    }

    private List<String> createNestingOrderWithNoTermField() {
        List<String> nestingOrder = new ArrayList<>();
        nestingOrder.add("time");
        nestingOrder.add("responsetime");
        return nestingOrder;
    }

    private Map<String, Integer> createFieldMapWithOneTermField() {
        Map<String, Integer> fieldMap = new HashMap<>();
        fieldMap.put("airline", 0);
        fieldMap.put("doc_count", 1);
        fieldMap.put("responsetime", 2);
        fieldMap.put("time", 3);
        return fieldMap;
    }

    private List<String> createNestingOrderWithOneTermField() {
        List<String> nestingOrder = new ArrayList<>();
        nestingOrder.add("time");
        nestingOrder.add("airline");
        nestingOrder.add("responsetime");
        return nestingOrder;
    }

    private Map<String, Integer> createFieldMapWithTwoTermFields() {
        Map<String, Integer> fieldMap = new HashMap<>();
        fieldMap.put("airline", 0);
        fieldMap.put("doc_count", 1);
        fieldMap.put("responsetime", 2);
        fieldMap.put("time", 3);
        fieldMap.put("sourcetype", 4);
        return fieldMap;
    }

    private List<String> createNestingOrderWithTwoTermFields() {
        List<String> nestingOrder = new ArrayList<>();
        nestingOrder.add("time");
        nestingOrder.add("sourcetype");
        nestingOrder.add("airline");
        nestingOrder.add("responsetime");
        return nestingOrder;
    }

}
