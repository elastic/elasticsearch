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
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * This class contains integration tests for searching rollup indices and dastreams containing rollups.
 */
public class RollupSearchIT extends ESRestTestCase {

    @SuppressWarnings("unchecked")
    public void testDatastreamRollupSearch() throws Exception {
        Template template = new Template(Settings.builder().put("index.number_of_shards", 1).build(),
            new CompressedXContent("{" +
                "\"properties\": {\n" +
                "  \"@timestamp\": { \"type\": \"date\" },\n" +
                "  \"units\": { \"type\": \"keyword\"}  \n" +
                "}}"), null);
        createComposableTemplate(client(), "logs-template", "logs-foo*", template);
        String dataStream = "logs-foo";
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-01-04T12:10:30Z\", \"temperature\": 27.5, \"units\": \"celcius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-01-08T07:12:25Z\", \"temperature\": 28.1, \"units\": \"celcius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-02-02T11:05:37Z\", \"temperature\": 29.2, \"units\": \"celcius\" }");
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-02-10T08:05:20Z\", \"temperature\": 19.5, \"units\": \"celcius\" }");
        rolloverMaxOneDocCondition(client(), dataStream);
        String firstGenerationIndex = DataStream.getDefaultBackingIndexName(dataStream, 1);
        String firstGenerationRollupIndex = ".rollup-" + firstGenerationIndex;
        assertBusy(() -> assertThat(indexExists(DataStream.getDefaultBackingIndexName(dataStream, 2)), is(true)), 30, TimeUnit.SECONDS);
        rollupIndex(client(), firstGenerationIndex, firstGenerationRollupIndex, "{\n" +
            "  \"groups\" : {\n" +
            "    \"date_histogram\": {\n" +
            "      \"field\": \"@timestamp\",\n" +
            "      \"calendar_interval\": \"1M\"\n" +
            "    },\n" +
            "    \"terms\": {\n" +
            "      \"fields\": [\"units\"]\n" +
            "    }\n" +
            "  },\n" +
            "  \"metrics\": [\n" +
            "    {\n" +
            "      \"field\": \"temperature\",\n" +
            "      \"metrics\": [\"max\", \"sum\", \"avg\"]\n" +
            "    }\n" +
            "  ]\n" +
            "}");
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
        Map<String, Object> aggs = (Map<String, Object>) response.get("aggregations");
        Map<String, Object> monthlyTemps = (Map<String, Object>) aggs.get("monthly_temperatures");
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) monthlyTemps.get("buckets");
        assertThat(buckets.size(), equalTo(2));
        assertThat(buckets.get(0).get("doc_count"), equalTo(2));
        assertEquals(28.1, (Double) ((Map<String, Object>) buckets.get(0).get("max_temperature")).get("value"), 0.0001);
        assertEquals(27.8, (Double) ((Map<String, Object>) buckets.get(0).get("avg_temperature")).get("value"), 0.0001);
        assertThat(buckets.get(1).get("doc_count"), equalTo(2));

        // Index new doc so that we confirm merging live and rollup indices
        indexDocument(client(), dataStream,
            "{ \"@timestamp\": \"2020-01-30T12:10:30Z\", \"temperature\": 20, \"units\": \"celcius\" }");
        response = search(dataStream, query);
        aggs = (Map<String, Object>) response.get("aggregations");
        monthlyTemps = (Map<String, Object>) aggs.get("monthly_temperatures");
        buckets = (List<Map<String, Object>>) monthlyTemps.get("buckets");
        assertThat(buckets.get(0).get("doc_count"), equalTo(3));
        assertEquals(28.1, (Double) ((Map<String, Object>) buckets.get(0).get("max_temperature")).get("value"), 0.0001);
        assertEquals(25.2, (Double) ((Map<String, Object>) buckets.get(0).get("avg_temperature")).get("value"), 0.0001);
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
