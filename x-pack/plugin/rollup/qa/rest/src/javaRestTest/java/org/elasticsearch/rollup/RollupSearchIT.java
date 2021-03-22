/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.rollup;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * This class contains integration tests for searching rollup indices and data streams containing rollups.
 */
public class RollupSearchIT extends ESRestTestCase {

    @SuppressWarnings("unchecked")
    public void testSearchRollupDatastream() throws Exception {
        Template template = new Template(Settings.builder().put("index.number_of_shards", 1).build(),
            new CompressedXContent("{" +
                "\"properties\": {\n" +
                "  \"@timestamp\": { \"type\": \"date\" },\n" +
                "  \"units\": { \"type\": \"keyword\"}  \n" +
                "}}"), null);
        createComposableTemplate(client(), "logs-template", "logs-foo*", template);
        String dataStream = "logs-foo";
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-01-04T12:10:30Z\", \"temperature\": 27.5, \"units\": \"celsius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-01-08T07:12:25Z\", \"temperature\": 28.1, \"units\": \"celsius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-02-02T11:05:37Z\", \"temperature\": 29.2, \"units\": \"celsius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-02-10T08:05:20Z\", \"temperature\": 19.5, \"units\": \"celsius\" }");
        rolloverMaxOneDocCondition(client(), dataStream);
        String firstGenerationIndex = DataStream.getDefaultBackingIndexName(dataStream, 1);
        String firstGenerationRollupIndex = ".rollup-" + firstGenerationIndex;
        assertBusy(() -> assertThat(indexExists(DataStream.getDefaultBackingIndexName(dataStream, 2)), is(true)), 30, TimeUnit.SECONDS);
        rollupIndex(client(), firstGenerationIndex, firstGenerationRollupIndex, "1M",
            Set.of("units"), Set.of("temperature"), Set.of("max", "sum", "avg"));
        String query = "{\n" +
            "  \"size\": 0,\n" +
            "  \"aggs\": {\n" +
            "      \"monthly_temperatures\": {\n" +
            "          \"date_histogram\": {\n" +
            "              \"field\": \"@timestamp\",\n" +
            "              \"calendar_interval\": \"1M\"\n" +
            "          },\n" +
            "          \"aggs\": {\n" +
            "              \"avg_temperature\": {\n" +
            "                  \"avg\": {\n" +
            "                      \"field\": \"temperature\"\n" +
            "                  }\n" +
            "              },\n" +
            "              \"max_temperature\": {\n" +
            "                  \"max\": {\n" +
            "                      \"field\": \"temperature\"\n" +
            "                  }\n" +
            "              }\n" +
            "          }\n" +
            "      }\n" +
            "  }\n" +
            "}";
        Map<String, Object> response = search(dataStream, query);
        Map<String, Object> shards = (Map<String, Object>) response.get("_shards");
        assertThat(shards.get("skipped"), equalTo(1));

        // Total hits should only contain the rollup docs (and not the live docs
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        assertThat(hits.get("total"), equalTo(2));
        Map<String, Object> aggs = (Map<String, Object>) response.get("aggregations");
        Map<String, Object> monthlyTemps = (Map<String, Object>) aggs.get("monthly_temperatures");
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) monthlyTemps.get("buckets");
        assertThat(buckets.size(), equalTo(2));
        assertThat(buckets.get(0).get("doc_count"), equalTo(2));
        assertEquals(28.1, (Double) ((Map<String, Object>) buckets.get(0).get("max_temperature")).get("value"), 0.0001);
        assertEquals(27.8, (Double) ((Map<String, Object>) buckets.get(0).get("avg_temperature")).get("value"), 0.0001);
        assertThat(buckets.get(1).get("doc_count"), equalTo(2));

        // Index new doc so that we confirm merging live and rollup indices
        // The following operation adds a new doc in the first bucket
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-01-30T12:10:30Z\", \"temperature\": 19.4, \"units\": \"celsius\" }");
        // The following operation adds a new doc in a new bucket
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-03-10T02:10:30Z\", \"temperature\": 23.5, \"units\": \"celsius\" }");
        response = search(dataStream, query);
        aggs = (Map<String, Object>) response.get("aggregations");
        monthlyTemps = (Map<String, Object>) aggs.get("monthly_temperatures");
        buckets = (List<Map<String, Object>>) monthlyTemps.get("buckets");
        assertThat(buckets.size(), equalTo(3));
        assertThat(buckets.get(0).get("doc_count"), equalTo(3));
        assertEquals(28.1, (Double) ((Map<String, Object>) buckets.get(0).get("max_temperature")).get("value"), 0.0001);
        assertEquals(25.0, (Double) ((Map<String, Object>) buckets.get(0).get("avg_temperature")).get("value"), 0.0001);
        assertThat(buckets.get(2).get("doc_count"), equalTo(1));
        assertEquals(23.5, (Double) ((Map<String, Object>) buckets.get(2).get("max_temperature")).get("value"), 0.0001);
        assertEquals(23.5, (Double) ((Map<String, Object>) buckets.get(2).get("avg_temperature")).get("value"), 0.0001);
    }

    /**
     * Test that queries a concrete live index and its concrete rollup index using
     * index wildcard in the search. Results from both live and rollup data should be
     * returned.
     */
    @SuppressWarnings("unchecked")
    public void testSearchConcreteRollupIndices() throws Exception {
        String liveIndex = "logs-foo";
        Settings settings = Settings.builder().put("index.number_of_shards", 1).build();
        String mapping =
            "\"properties\": {\n" +
                "  \"@timestamp\": { \"type\": \"date\" },\n" +
                "  \"units\": { \"type\": \"keyword\"},  \n" +
                "  \"temperature\": { \"type\": \"double\"}  \n" +
                "}";
        createIndex(liveIndex, settings, mapping);

        indexDocument(client(), liveIndex,
            "{ \"@timestamp\": \"2020-01-04T12:10:30Z\", \"temperature\": 27.5, \"units\": \"celsius\" }");
        indexDocument(client(), liveIndex,
            "{ \"@timestamp\": \"2020-01-08T07:12:25Z\", \"temperature\": 28.1, \"units\": \"celsius\" }");
        indexDocument(client(), liveIndex,
            "{ \"@timestamp\": \"2020-02-02T11:05:37Z\", \"temperature\": 29.2, \"units\": \"celsius\" }");
        indexDocument(client(), liveIndex,
            "{ \"@timestamp\": \"2020-02-10T08:05:20Z\", \"temperature\": 19.5, \"units\": \"celsius\" }");
        String rollupIndex = "rollup-" + liveIndex;

        rollupIndex(client(), liveIndex, rollupIndex, "1M",
            Set.of("units"), Set.of("temperature"), Set.of("max", "sum", "avg"));

        String query = "{\n" +
            "  \"size\": 0,\n" +
            "  \"aggs\": {\n" +
            "      \"monthly_temperatures\": {\n" +
            "          \"date_histogram\": {\n" +
            "              \"field\": \"@timestamp\",\n" +
            "              \"calendar_interval\": \"1M\"\n" +
            "          },\n" +
            "          \"aggs\": {\n" +
            "              \"avg_temperature\": {\n" +
            "                  \"avg\": {\n" +
            "                      \"field\": \"temperature\"\n" +
            "                  }\n" +
            "              },\n" +
            "              \"max_temperature\": {\n" +
            "                  \"max\": {\n" +
            "                      \"field\": \"temperature\"\n" +
            "                  }\n" +
            "              }\n" +
            "          }\n" +
            "      }\n" +
            "  }\n" +
            "}";
        String indices = "*" + liveIndex + "*";
        Map<String, Object> response = search(indices, query);
        Map<String, Object> shards = (Map<String, Object>) response.get("_shards");
        assertThat(shards.get("skipped"), equalTo(0));
        assertThat(shards.get("successful"), equalTo(2));
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        assertThat(hits.get("total"), equalTo(6)); // 4 docs in the live index + 2 docs in the rollup index

        Map<String, Object> aggs = (Map<String, Object>) response.get("aggregations");
        Map<String, Object> monthlyTemps = (Map<String, Object>) aggs.get("monthly_temperatures");
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) monthlyTemps.get("buckets");
        assertThat(buckets.size(), equalTo(2));
        assertThat(buckets.get(0).get("doc_count"), equalTo(4));
        assertEquals(28.1, (Double) ((Map<String, Object>) buckets.get(0).get("max_temperature")).get("value"), 0.0001);
        assertEquals(27.8, (Double) ((Map<String, Object>) buckets.get(0).get("avg_temperature")).get("value"), 0.0001);
        assertThat(buckets.get(1).get("doc_count"), equalTo(4));

        // Delete the index and run the query again. Now only the rollup index should match the query
        deleteIndex(liveIndex);
        response = search(indices, query);
        hits = (Map<String, Object>) response.get("hits");
        assertThat(hits.get("total"), equalTo(2));
        aggs = (Map<String, Object>) response.get("aggregations");
        monthlyTemps = (Map<String, Object>) aggs.get("monthly_temperatures");
        buckets = (List<Map<String, Object>>) monthlyTemps.get("buckets");
        assertThat(buckets.size(), equalTo(2)); // 2 docs in the rollup index
        assertThat(buckets.get(0).get("doc_count"), equalTo(2));
        assertEquals(28.1, (Double) ((Map<String, Object>) buckets.get(0).get("max_temperature")).get("value"), 0.0001);
        assertEquals(27.8, (Double) ((Map<String, Object>) buckets.get(0).get("avg_temperature")).get("value"), 0.0001);
        assertThat(buckets.get(1).get("doc_count"), equalTo(2));
    }

    @SuppressWarnings("unchecked")
    public void testSearchMultipleRollupIntervals() throws Exception {
        Template template = new Template(Settings.builder().put("index.number_of_shards", 1).build(),
            new CompressedXContent("{" +
                "\"properties\": {\n" +
                "  \"@timestamp\": { \"type\": \"date\" },\n" +
                "  \"units\": { \"type\": \"keyword\"},  \n" +
                "  \"temperature\": { \"type\": \"double\"}  \n" +
                "}}"), null);
        createComposableTemplate(client(), "logs-template", "logs-foo*", template);
        String dataStream = "logs-foo";
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-01-04T12:10:30Z\", \"temperature\": 27.5, \"units\": \"celsius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-01-04T17:12:25Z\", \"temperature\": 28.1, \"units\": \"celsius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-01-17T11:12:25Z\", \"temperature\": 25.4, \"units\": \"celsius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-02-02T11:00:37Z\", \"temperature\": 29, \"units\": \"celsius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-02-02T11:05:37Z\", \"temperature\": 27, \"units\": \"celsius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-02-17T08:05:20Z\", \"temperature\": 19.5, \"units\": \"celsius\" }");

        rolloverMaxOneDocCondition(client(), dataStream);
        String firstGenerationIndex = DataStream.getDefaultBackingIndexName(dataStream, 1);
        assertBusy(() -> assertThat(indexExists(DataStream.getDefaultBackingIndexName(dataStream, 2)), is(true)), 30, TimeUnit.SECONDS);
        // Rollup daily and monthly intervals
        String dailyRollupIndex = ".rollup-daily-" + firstGenerationIndex;
        rollupIndex(client(), firstGenerationIndex, dailyRollupIndex, "1d",
            Set.of("units"), Set.of("temperature"), Set.of("max", "sum", "avg"));
        String monthlyRollupIndex = ".rollup-monthly-" + firstGenerationIndex;
        rollupIndex(client(), firstGenerationIndex, monthlyRollupIndex, "1M",
            Set.of("units"), Set.of("temperature"), Set.of("max", "sum", "avg"));

        String query = "{\n" +
            "  \"size\": 0,\n" +
            "  \"aggs\": {\n" +
            "      \"temperatures\": {\n" +
            "          \"date_histogram\": {\n" +
            "              \"field\": \"@timestamp\",\n" +
            "              \"calendar_interval\": \"%s\",\n" +
            "              \"min_doc_count\": 1\n" +
            "          },\n" +
            "          \"aggs\": {\n" +
            "              \"avg_temperature\": {\n" +
            "                  \"avg\": {\n" +
            "                      \"field\": \"temperature\"\n" +
            "                  }\n" +
            "              }\n" +
            "          }\n" +
            "      }\n" +
            "  }\n" +
            "}";
        Map<String, Object> response = search(dataStream, String.format(query, "1M"));
        Map<String, Object> shards = (Map<String, Object>) response.get("_shards");
        assertThat(shards.get("skipped"), equalTo(2));

        // Total hits should only contain the docs in the monthly rollup index
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        assertThat(hits.get("total"), equalTo(2)); // 2 docs in the monthly rollup index
        Map<String, Object> aggs = (Map<String, Object>) response.get("aggregations");
        Map<String, Object> temps = (Map<String, Object>) aggs.get("temperatures");
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) temps.get("buckets");
        assertThat(buckets.size(), equalTo(2));
        assertThat(buckets.get(0).get("doc_count"), equalTo(3));
        assertEquals(27, (Double) ((Map<String, Object>) buckets.get(0).get("avg_temperature")).get("value"), 0.0001);
        assertThat(buckets.get(1).get("doc_count"), equalTo(3));

        // Search for daily results. It should return the results from daily rollup index
        response =  search(dataStream, String.format(query, "1d"));
        shards = (Map<String, Object>) response.get("_shards");
        assertThat(shards.get("skipped"), equalTo(2));
        hits = (Map<String, Object>) response.get("hits");
        assertThat(hits.get("total"), equalTo(4)); // 4 docs in the daily rollup index

        aggs = (Map<String, Object>) response.get("aggregations");
        temps = (Map<String, Object>) aggs.get("temperatures");
        buckets = (List<Map<String, Object>>) temps.get("buckets");
        assertThat(buckets.size(), equalTo(4));
        assertThat(buckets.get(0).get("doc_count"), equalTo(2));
        assertEquals(27.8, (Double) ((Map<String, Object>) buckets.get(0).get("avg_temperature")).get("value"), 0.0001);
        assertThat(buckets.get(1).get("doc_count"), equalTo(1));
        assertThat(buckets.get(2).get("doc_count"), equalTo(2));
        assertThat(buckets.get(3).get("doc_count"), equalTo(1));

        // Search for hourly results. It should return the results from live index, because there are no
        // rollups that satisfy the hourly interval
        response =  search(dataStream, String.format(query, "1h"));
        shards = (Map<String, Object>) response.get("_shards");
        assertThat(shards.get("skipped"), equalTo(2));
        hits = (Map<String, Object>) response.get("hits");
        assertThat(hits.get("total"), equalTo(6)); // 6 docs in the live index
    }

    /**
     * Test that when querying explitly the indices inside a datastream, no result merging occurs
     * and result from all indices are returned.
     */
    @SuppressWarnings("unchecked")
    public void testSearchConcreteRollupIndicesInDatastream() throws Exception {
        Template template = new Template(Settings.builder().put("index.number_of_shards", 1).build(),
            new CompressedXContent("{" +
                "\"properties\": {\n" +
                "  \"@timestamp\": { \"type\": \"date\" },\n" +
                "  \"units\": { \"type\": \"keyword\"},  \n" +
                "  \"temperature\": { \"type\": \"double\"}  \n" +
                "}}"), null);
        createComposableTemplate(client(), "logs-template", "logs-foo*", template);
        String dataStream = "logs-foo";
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-01-04T12:10:30Z\", \"temperature\": 27.5, \"units\": \"celsius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-01-04T17:12:25Z\", \"temperature\": 28.1, \"units\": \"celsius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-01-17T11:12:25Z\", \"temperature\": 25.4, \"units\": \"celsius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-02-02T11:00:37Z\", \"temperature\": 29, \"units\": \"celsius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-02-02T11:05:37Z\", \"temperature\": 27, \"units\": \"celsius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-02-17T08:05:20Z\", \"temperature\": 19.5, \"units\": \"celsius\" }");

        rolloverMaxOneDocCondition(client(), dataStream);
        String firstGenerationIndex = DataStream.getDefaultBackingIndexName(dataStream, 1);
        assertBusy(() -> assertThat(indexExists(DataStream.getDefaultBackingIndexName(dataStream, 2)), is(true)), 30, TimeUnit.SECONDS);
        // Rollup daily and monthly intervals
        String dailyRollupIndex = ".rollup-daily-" + firstGenerationIndex;
        rollupIndex(client(), firstGenerationIndex, dailyRollupIndex, "1d",
            Set.of("units"), Set.of("temperature"), Set.of("max", "sum", "avg"));
        String monthlyRollupIndex = ".rollup-monthly-" + firstGenerationIndex;
        rollupIndex(client(), firstGenerationIndex, monthlyRollupIndex, "1M",
            Set.of("units"), Set.of("temperature"), Set.of("max", "sum", "avg"));

        String query = "{\n" +
            "  \"size\": 0,\n" +
            "  \"aggs\": {\n" +
            "      \"temperatures\": {\n" +
            "          \"date_histogram\": {\n" +
            "              \"field\": \"@timestamp\",\n" +
            "              \"calendar_interval\": \"%s\",\n" +
            "              \"min_doc_count\": 1\n" +
            "          },\n" +
            "          \"aggs\": {\n" +
            "              \"avg_temperature\": {\n" +
            "                  \"avg\": {\n" +
            "                      \"field\": \"temperature\"\n" +
            "                  }\n" +
            "              }\n" +
            "          }\n" +
            "      }\n" +
            "  }\n" +
            "}";

        // Query daily rollup index explicitly instead of the data stream. Even if monthly rollups
        // index exists, results should be computed on the daily rollups
        Map<String, Object> response = search(dailyRollupIndex, String.format(query, "1M"));
        Map<String, Object> shards = (Map<String, Object>) response.get("_shards");
        assertThat(shards.get("total"), equalTo(1));
        assertThat(shards.get("skipped"), equalTo(0));

        // Total hits should contain the docs in the daily rollup index
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        assertThat(hits.get("total"), equalTo(4)); // 4 docs in the daily rollup index
        Map<String, Object> aggs = (Map<String, Object>) response.get("aggregations");
        Map<String, Object> temps = (Map<String, Object>) aggs.get("temperatures");
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) temps.get("buckets");
        assertThat(buckets.size(), equalTo(2));
        assertThat(buckets.get(0).get("doc_count"), equalTo(3));
        assertEquals(27, (Double) ((Map<String, Object>) buckets.get(0).get("avg_temperature")).get("value"), 0.0001);
        assertThat(buckets.get(1).get("doc_count"), equalTo(3));

        // Search for monthly results in the live index. No rollups should be queried.
        response =  search(firstGenerationIndex, String.format(query, "1M"));
        shards = (Map<String, Object>) response.get("_shards");
        assertThat(shards.get("total"), equalTo(1));
        assertThat(shards.get("skipped"), equalTo(0));
        hits = (Map<String, Object>) response.get("hits");
        assertThat(hits.get("total"), equalTo(6)); // 6 docs in the live index

        aggs = (Map<String, Object>) response.get("aggregations");
        temps = (Map<String, Object>) aggs.get("temperatures");
        buckets = (List<Map<String, Object>>) temps.get("buckets");
        assertThat(buckets.size(), equalTo(2));
        assertThat(buckets.get(0).get("doc_count"), equalTo(3));
        assertEquals(27, (Double) ((Map<String, Object>) buckets.get(0).get("avg_temperature")).get("value"), 0.0001);
        assertThat(buckets.get(1).get("doc_count"), equalTo(3));

        // Search for monthly results  in all indices. It should return documents for all indices
        String allIndices = firstGenerationIndex + "," + dailyRollupIndex + "," + monthlyRollupIndex;
        response =  search(allIndices, String.format(query, "1M"));
        shards = (Map<String, Object>) response.get("_shards");
        assertThat(shards.get("total"), equalTo(3));
        assertThat(shards.get("skipped"), equalTo(0));
        hits = (Map<String, Object>) response.get("hits");
        assertThat(hits.get("total"), equalTo(12)); // 6 + 4 + 2 docs in the live, daily and monthly index
    }

    /**
     * Rollup a datastream with the default timezone and aggregate using different timezones in the
     * date_histogram
     */
    @SuppressWarnings("unchecked")
    public void testSearchRollupDifferentTimezone() throws Exception {
        Template template = new Template(Settings.builder().put("index.number_of_shards", 1).build(),
            new CompressedXContent("{" +
                "\"properties\": {\n" +
                "  \"@timestamp\": { \"type\": \"date\" },\n" +
                "  \"units\": { \"type\": \"keyword\"},  \n" +
                "  \"temperature\": { \"type\": \"double\"}  \n" +
                "}}"), null);
        createComposableTemplate(client(), "logs-template", "logs-foo*", template);
        String dataStream = "logs-foo";
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-01-04T00:10:30Z\", \"temperature\": 27.5, \"units\": \"celsius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-01-04T23:12:25Z\", \"temperature\": 28.1, \"units\": \"celsius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-01-05T01:12:25Z\", \"temperature\": 25.4, \"units\": \"celsius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-01-06T11:00:37Z\", \"temperature\": 29, \"units\": \"celsius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-01-07T11:05:37Z\", \"temperature\": 27, \"units\": \"celsius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-01-07T23:05:20Z\", \"temperature\": 19.5, \"units\": \"celsius\" }");

        rolloverMaxOneDocCondition(client(), dataStream);
        String firstGenerationIndex = DataStream.getDefaultBackingIndexName(dataStream, 1);
        assertBusy(() -> assertThat(indexExists(DataStream.getDefaultBackingIndexName(dataStream, 2)), is(true)), 30, TimeUnit.SECONDS);

        // Rollup daily. Default rollup is in UTC timezone.
        String dailyRollupIndex = ".rollup-utc-daily-" + firstGenerationIndex;
        rollupIndex(client(), firstGenerationIndex, dailyRollupIndex, "1d",
            Set.of("units"), Set.of("temperature"), Set.of("max", "sum", "avg"));

        String query = "{\n" +
            "  \"size\": 0,\n" +
            "  \"aggs\": {\n" +
            "      \"temperatures\": {\n" +
            "          \"date_histogram\": {\n" +
            "              \"field\": \"@timestamp\",\n" +
            "              \"calendar_interval\": \"1d\",\n" +
            "              \"time_zone\": \"%s\",\n" +
            "              \"min_doc_count\": 1\n" +
            "          },\n" +
            "          \"aggs\": {\n" +
            "              \"avg_temperature\": {\n" +
            "                  \"avg\": {\n" +
            "                      \"field\": \"temperature\"\n" +
            "                  }\n" +
            "              }\n" +
            "          }\n" +
            "      }\n" +
            "  }\n" +
            "}";
        // Search for daily results with the default tz. It should return the results from daily rollup index
        Map<String, Object> response = search(dataStream, String.format(query, "+00:00"));
        Map<String, Object> shards = (Map<String, Object>) response.get("_shards");
        assertThat(shards.get("skipped"), equalTo(1));

        // Total hits should only contain the docs in the daily rollup index
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        assertThat(hits.get("total"), equalTo(4)); // 4 docs in the daily rollup index
        Map<String, Object> aggs = (Map<String, Object>) response.get("aggregations");
        Map<String, Object> temps = (Map<String, Object>) aggs.get("temperatures");
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) temps.get("buckets");
        assertThat(buckets.size(), equalTo(4));
        assertThat(buckets.get(0).get("doc_count"), equalTo(2));
        assertThat(buckets.get(1).get("doc_count"), equalTo(1));
        assertThat(buckets.get(2).get("doc_count"), equalTo(1));
        assertThat(buckets.get(3).get("doc_count"), equalTo(2));

        // Search for daily results with different tz than rollups. It should return the results from the original index,
        // because there are no rollups that satisfy the the timezone
        response =  search(dataStream, String.format(query, "-02:00"));
        shards = (Map<String, Object>) response.get("_shards");
        assertThat(shards.get("skipped"), equalTo(1));
        hits = (Map<String, Object>) response.get("hits");
        assertThat(hits.get("total"), equalTo(6)); // 6 docs in the live index
        aggs = (Map<String, Object>) response.get("aggregations");
        temps = (Map<String, Object>) aggs.get("temperatures");
        buckets = (List<Map<String, Object>>) temps.get("buckets");
        assertThat(buckets.size(), equalTo(4));
        assertThat(buckets.get(0).get("doc_count"), equalTo(1));
        assertEquals(27.5, (Double) ((Map<String, Object>) buckets.get(0).get("avg_temperature")).get("value"), 0.0001);
        assertThat(buckets.get(1).get("doc_count"), equalTo(2));
    }

    @AwaitsFix(bugUrl = "TODO")
    @SuppressWarnings("unchecked")
    public void testSearchRollupDatastreamMissingMetric() throws Exception {
        Template template = new Template(Settings.builder().put("index.number_of_shards", 1).build(),
            new CompressedXContent("{" +
                "\"properties\": {\n" +
                "  \"@timestamp\": { \"type\": \"date\" },\n" +
                "  \"units\": { \"type\": \"keyword\"}  \n" +
                "}}"), null);
        createComposableTemplate(client(), "logs-template", "logs-foo*", template);
        String dataStream = "logs-foo";
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-01-04T12:10:30Z\", \"temperature\": 27.5, \"units\": \"celsius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-01-08T07:12:25Z\", \"temperature\": 28.1, \"units\": \"celsius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-02-02T11:05:37Z\", \"temperature\": 29.2, \"units\": \"celsius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-02-10T08:05:20Z\", \"temperature\": 19.5, \"units\": \"celsius\" }");
        rolloverMaxOneDocCondition(client(), dataStream);
        String firstGenerationIndex = DataStream.getDefaultBackingIndexName(dataStream, 1);
        String firstGenerationRollupIndex = ".rollup-" + firstGenerationIndex;
        assertBusy(() -> assertThat(indexExists(DataStream.getDefaultBackingIndexName(dataStream, 2)), is(true)), 30, TimeUnit.SECONDS);
        rollupIndex(client(), firstGenerationIndex, firstGenerationRollupIndex, "1M",
            Set.of("units"), Set.of("temperature"), Set.of("max", "sum"));
        String query = "{\n" +
            "  \"size\": 0,\n" +
            "  \"aggs\": {\n" +
            "      \"monthly_temperatures\": {\n" +
            "          \"date_histogram\": {\n" +
            "              \"field\": \"@timestamp\",\n" +
            "              \"calendar_interval\": \"1M\"\n" +
            "          },\n" +
            "          \"aggs\": {\n" +
            "              \"min_temperature\": {\n" +
            "                  \"min\": {\n" +
            "                      \"field\": \"temperature\"\n" +
            "                  }\n" +
            "              }\n" +
            "          }\n" +
            "      }\n" +
            "  }\n" +
            "}";
        Map<String, Object> response = search(dataStream, query);
        Map<String, Object> shards = (Map<String, Object>) response.get("_shards");
        assertThat(shards.get("skipped"), equalTo(1));

        // Total hits should only contain the live docs (and not the rollup docs)
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        assertThat(hits.get("total"), equalTo(4));
        Map<String, Object> aggs = (Map<String, Object>) response.get("aggregations");
        Map<String, Object> monthlyTemps = (Map<String, Object>) aggs.get("monthly_temperatures");
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) monthlyTemps.get("buckets");
        assertThat(buckets.size(), equalTo(2));
        assertThat(buckets.get(0).get("doc_count"), equalTo(2));
        assertEquals(27.5, (Double) ((Map<String, Object>) buckets.get(0).get("min_temperature")).get("value"), 0.0001);
        assertThat(buckets.get(1).get("doc_count"), equalTo(2));
        assertEquals(19.5, (Double) ((Map<String, Object>) buckets.get(1).get("min_temperature")).get("value"), 0.0001);

        // Index new doc so that we confirm merging live and rollup indices
        // The following operation adds a new doc in the first bucket
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-01-30T12:10:30Z\", \"temperature\": 19.4, \"units\": \"celsius\" }");
        // The following operation adds a new doc in a new bucket
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-03-10T02:10:30Z\", \"temperature\": 23.5, \"units\": \"celsius\" }");
        response = search(dataStream, query);
        aggs = (Map<String, Object>) response.get("aggregations");
        monthlyTemps = (Map<String, Object>) aggs.get("monthly_temperatures");
        buckets = (List<Map<String, Object>>) monthlyTemps.get("buckets");
        assertThat(buckets.size(), equalTo(3));
        assertThat(buckets.get(0).get("doc_count"), equalTo(3));
        assertEquals(28.1, (Double) ((Map<String, Object>) buckets.get(0).get("max_temperature")).get("value"), 0.0001);
        assertEquals(25.0, (Double) ((Map<String, Object>) buckets.get(0).get("avg_temperature")).get("value"), 0.0001);
        assertThat(buckets.get(2).get("doc_count"), equalTo(1));
        assertEquals(23.5, (Double) ((Map<String, Object>) buckets.get(2).get("max_temperature")).get("value"), 0.0001);
        assertEquals(23.5, (Double) ((Map<String, Object>) buckets.get(2).get("avg_temperature")).get("value"), 0.0001);
    }

    private static void createComposableTemplate(RestClient client, String templateName, String indexPattern, Template template)
        throws IOException {
        XContentBuilder builder = jsonBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        StringEntity templateJSON = new StringEntity(
            String.format(Locale.ROOT, "{\n" +
                "  \"index_patterns\": \"%s\",\n" +
                "  \"data_stream\": {},\n" +
                "  \"template\": %s\n" +
                "}", indexPattern, Strings.toString(builder)),
            ContentType.APPLICATION_JSON);
        Request createIndexTemplateRequest = new Request("PUT", "_index_template/" + templateName);
        createIndexTemplateRequest.setEntity(templateJSON);
        client.performRequest(createIndexTemplateRequest);
    }

    private static void rolloverMaxOneDocCondition(RestClient client, String indexAbstractionName) throws IOException {
        Request rolloverRequest = new Request("POST", "/" + indexAbstractionName + "/_rollover");
        rolloverRequest.setJsonEntity("{\n" +
            "  \"conditions\": {\n" +
            "    \"max_docs\": \"1\"\n" +
            "  }\n" +
            "}"
        );
        client.performRequest(rolloverRequest);
    }

    private static void rollupIndex(RestClient client, String indexAbstractionName, String rollupIndex,
                                    String rollupConfig) throws IOException {
        Request rollupRequest = new Request("POST", "/" + indexAbstractionName + "/_rollup/" + rollupIndex);
        rollupRequest.setJsonEntity(rollupConfig);
        client.performRequest(rollupRequest);
    }

    private static void rollupIndex(RestClient client, String indexAbstractionName, String rollupIndex,
                                    String interval, Set<String> terms, Set<String> metricFields, Set<String> metrics) throws IOException {
        String rollupConfig = "{\n" +
            "  \"groups\" : {\n" +
            "    \"date_histogram\": {\n" +
            "      \"field\": \"@timestamp\",\n" +
            "      \"calendar_interval\": \"" + interval + "\"\n" +
            "    },\n" +
            "    \"terms\": {\n" +
            "      \"fields\": [\"" + String.join("\", \"", terms) + "\"]\n" +
            "    }\n" +
            "  },\n" +
            "  \"metrics\": [\n" +
            "    {\n" +
            "      \"field\": \"temperature\",\n" +
            "      \"metrics\": [\"max\", \"sum\", \"avg\"]\n" +
            "    }\n" +
            "  ]\n" +
            "}";
        rollupIndex(client, indexAbstractionName, rollupIndex, rollupConfig);
    }

    private static void indexDocument(RestClient client, String indexAbstractionName, String documentString) throws IOException {
        Request indexRequest = new Request("POST", indexAbstractionName + "/_doc?refresh");
        indexRequest.setEntity(new StringEntity(documentString, ContentType.APPLICATION_JSON));
        client.performRequest(indexRequest);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> search(String indexAbstractionName, String query) throws IOException {
        Request searchForStats = new Request("GET", indexAbstractionName + "/_search?rest_total_hits_as_int");
        searchForStats.setJsonEntity(query);
        Response searchResponse = client().performRequest(searchForStats);
        Map<String, Object> responseAsMap = entityAsMap(searchResponse);
        return responseAsMap;
    }
}
