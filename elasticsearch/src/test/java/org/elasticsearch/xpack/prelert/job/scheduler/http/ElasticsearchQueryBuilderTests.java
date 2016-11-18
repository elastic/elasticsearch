/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.scheduler.http;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.ESTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ElasticsearchQueryBuilderTests extends ESTestCase {

    public void testCreateSearchBody_GivenQueryOnly() {
        ElasticsearchQueryBuilder queryBuilder = new ElasticsearchQueryBuilder("\"match_all\":{}", null, null, null, "time");

        assertFalse(queryBuilder.isAggregated());

        String searchBody = queryBuilder.createSearchBody(1451606400000L, 1451610000000L);

        String expected = "{" + "  \"sort\": [" + "    {\"time\": {\"order\": \"asc\"}}" + "  ]," + "  \"query\": {" + "    \"bool\": {"
                + "      \"filter\": [" + "        {\"match_all\":{}}," + "        {" + "          \"range\": {" + "            \"time\": {"
                + "              \"gte\": \"2016-01-01T00:00:00.000Z\"," + "              \"lt\": \"2016-01-01T01:00:00.000Z\","
                + "              \"format\": \"date_time\"" + "            }" + "          }" + "        }" + "      ]" + "    }" + "  }"
                + "}";
        assertEquals(expected.replaceAll(" ", ""), searchBody.replaceAll(" ", ""));
    }

    public void testCreateSearchBody_GivenQueryAndFields() {
        ElasticsearchQueryBuilder queryBuilder = new ElasticsearchQueryBuilder("\"match_all\":{}", null, null, "[\"foo\",\"bar\"]",
                "@timestamp");

        assertFalse(queryBuilder.isAggregated());

        String searchBody = queryBuilder.createSearchBody(1451606400000L, 1451610000000L);

        String expected = "{" + "  \"sort\": [" + "    {\"@timestamp\": {\"order\": \"asc\"}}" + "  ]," + "  \"query\": {"
                + "    \"bool\": {" + "      \"filter\": [" + "        {\"match_all\":{}}," + "        {" + "          \"range\": {"
                + "            \"@timestamp\": {" + "              \"gte\": \"2016-01-01T00:00:00.000Z\","
                + "              \"lt\": \"2016-01-01T01:00:00.000Z\"," + "              \"format\": \"date_time\"" + "            }"
                + "          }" + "        }" + "      ]" + "    }" + "  }," + "  \"_source\": [\"foo\",\"bar\"]" + "}";
        assertEquals(expected.replaceAll(" ", ""), searchBody.replaceAll(" ", ""));
    }

    public void testCreateSearchBody_GivenQueryAndFieldsAndScriptFields() {
        ElasticsearchQueryBuilder queryBuilder = new ElasticsearchQueryBuilder("\"match_all\":{}", null,
                "{\"test1\":{\"script\": \"...\"}}", "[\"foo\",\"bar\"]", "@timestamp");

        assertFalse(queryBuilder.isAggregated());

        String searchBody = queryBuilder.createSearchBody(1451606400000L, 1451610000000L);

        String expected = "{" + "  \"sort\": [" + "    {\"@timestamp\": {\"order\": \"asc\"}}" + "  ]," + "  \"query\": {"
                + "    \"bool\": {" + "      \"filter\": [" + "        {\"match_all\":{}}," + "        {" + "          \"range\": {"
                + "            \"@timestamp\": {" + "              \"gte\": \"2016-01-01T00:00:00.000Z\","
                + "              \"lt\": \"2016-01-01T01:00:00.000Z\"," + "              \"format\": \"date_time\"" + "            }"
                + "          }" + "        }" + "      ]" + "    }" + "  }," + "  \"script_fields\": {\"test1\":{\"script\":\"...\"}},"
                + "  \"_source\": [\"foo\",\"bar\"]" + "}";
        assertEquals(expected.replaceAll(" ", ""), searchBody.replaceAll(" ", ""));
    }

    public void testCreateSearchBody_GivenQueryAndAggs() {
        ElasticsearchQueryBuilder queryBuilder = new ElasticsearchQueryBuilder("\"match_all\":{}", "{\"my_aggs\":{}}", null, null, "time");

        assertTrue(queryBuilder.isAggregated());

        String searchBody = queryBuilder.createSearchBody(1451606400000L, 1451610000000L);

        String expected = "{" + "  \"sort\": [" + "    {\"time\": {\"order\": \"asc\"}}" + "  ]," + "  \"query\": {" + "    \"bool\": {"
                + "      \"filter\": [" + "        {\"match_all\":{}}," + "        {" + "          \"range\": {" + "            \"time\": {"
                + "              \"gte\": \"2016-01-01T00:00:00.000Z\"," + "              \"lt\": \"2016-01-01T01:00:00.000Z\","
                + "              \"format\": \"date_time\"" + "            }" + "          }" + "        }" + "      ]" + "    }" + "  },"
                + "  \"aggs\":{\"my_aggs\":{}}" + "}";
        assertEquals(expected.replaceAll(" ", ""), searchBody.replaceAll(" ", ""));
    }

    public void testCreateDataSummaryQuery_GivenQueryOnly() {
        ElasticsearchQueryBuilder queryBuilder = new ElasticsearchQueryBuilder("\"match_all\":{}", null, null, null, "@timestamp");

        assertFalse(queryBuilder.isAggregated());

        String dataSummaryQuery = queryBuilder.createDataSummaryQuery(1451606400000L, 1451610000000L);

        String expected = "{" + "  \"sort\": [" + "    {\"_doc\": {\"order\": \"asc\"}}" + "  ]," + "  \"query\": {" + "    \"bool\": {"
                + "      \"filter\": [" + "        {\"match_all\":{}}," + "        {" + "          \"range\": {"
                + "            \"@timestamp\": {" + "              \"gte\": \"2016-01-01T00:00:00.000Z\","
                + "              \"lt\": \"2016-01-01T01:00:00.000Z\"," + "              \"format\": \"date_time\"" + "            }"
                + "          }" + "        }" + "      ]" + "    }" + "  }," + "  \"aggs\":{" + "    \"earliestTime\":{"
                + "      \"min\":{\"field\":\"@timestamp\"}" + "    }," + "    \"latestTime\":{"
                + "      \"max\":{\"field\":\"@timestamp\"}" + "    }" + "  }" + "}";
        assertEquals(expected.replaceAll(" ", ""), dataSummaryQuery.replaceAll(" ", ""));
    }

    public void testCreateDataSummaryQuery_GivenQueryAndFieldsAndScriptFields() {
        ElasticsearchQueryBuilder queryBuilder = new ElasticsearchQueryBuilder("\"match_all\":{}", null,
                "{\"test1\":{\"script\": \"...\"}}", "[\"foo\",\"bar\"]", "@timestamp");

        assertFalse(queryBuilder.isAggregated());

        String dataSummaryQuery = queryBuilder.createDataSummaryQuery(1451606400000L, 1451610000000L);

        String expected = "{" + "  \"sort\": [" + "    {\"_doc\": {\"order\": \"asc\"}}" + "  ]," + "  \"query\": {" + "    \"bool\": {"
                + "      \"filter\": [" + "        {\"match_all\":{}}," + "        {" + "          \"range\": {"
                + "            \"@timestamp\": {" + "              \"gte\": \"2016-01-01T00:00:00.000Z\","
                + "              \"lt\": \"2016-01-01T01:00:00.000Z\"," + "              \"format\": \"date_time\"" + "            }"
                + "          }" + "        }" + "      ]" + "    }" + "  }," + "  \"aggs\":{" + "    \"earliestTime\":{"
                + "      \"min\":{\"field\":\"@timestamp\"}" + "    }," + "    \"latestTime\":{"
                + "      \"max\":{\"field\":\"@timestamp\"}" + "    }" + "  }" + "}";
        assertEquals(expected.replaceAll(" ", ""), dataSummaryQuery.replaceAll(" ", ""));
    }

    public void testCreateDataSummaryQuery_GivenQueryAndAggs() {
        ElasticsearchQueryBuilder queryBuilder = new ElasticsearchQueryBuilder("\"match_all\":{}", "{\"my_aggs\":{}}", null, null,
                "@timestamp");

        assertTrue(queryBuilder.isAggregated());

        String dataSummaryQuery = queryBuilder.createDataSummaryQuery(1451606400000L, 1451610000000L);

        String expected = "{" + "  \"sort\": [" + "    {\"_doc\": {\"order\": \"asc\"}}" + "  ]," + "  \"query\": {" + "    \"bool\": {"
                + "      \"filter\": [" + "        {\"match_all\":{}}," + "        {" + "          \"range\": {"
                + "            \"@timestamp\": {" + "              \"gte\": \"2016-01-01T00:00:00.000Z\","
                + "              \"lt\": \"2016-01-01T01:00:00.000Z\"," + "              \"format\": \"date_time\"" + "            }"
                + "          }" + "        }" + "      ]" + "    }" + "  }," + "  \"aggs\":{" + "    \"earliestTime\":{"
                + "      \"min\":{\"field\":\"@timestamp\"}" + "    }," + "    \"latestTime\":{"
                + "      \"max\":{\"field\":\"@timestamp\"}" + "    }" + "  }" + "}";
        assertEquals(expected.replaceAll(" ", ""), dataSummaryQuery.replaceAll(" ", ""));
    }

    public void testLogQueryInfo_GivenNoAggsNoFields() {
        ElasticsearchQueryBuilder queryBuilder = new ElasticsearchQueryBuilder("\"match_all\":{}", null, null, null, "@timestamp");

        Logger logger = mock(Logger.class);
        queryBuilder.logQueryInfo(logger);

        verify(logger).debug("Will retrieve whole _source document from Elasticsearch");
    }

    public void testLogQueryInfo_GivenFields() {
        ElasticsearchQueryBuilder queryBuilder = new ElasticsearchQueryBuilder("\"match_all\":{}", null, null, "[\"foo\"]", "@timestamp");

        Logger logger = mock(Logger.class);
        queryBuilder.logQueryInfo(logger);

        verify(logger).debug("Will request only the following field(s) from Elasticsearch: [\"foo\"]");
    }

    public void testLogQueryInfo_GivenAggs() {
        ElasticsearchQueryBuilder queryBuilder = new ElasticsearchQueryBuilder("\"match_all\":{}", "{\"my_aggs\":{ \"foo\": \"bar\" }}",
                null, null, "@timestamp");

        Logger logger = mock(Logger.class);
        queryBuilder.logQueryInfo(logger);

        verify(logger).debug("Will use the following Elasticsearch aggregations: {\"my_aggs\":{ \"foo\": \"bar\" }}");
    }
}
