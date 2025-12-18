/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.entityToMap;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.GLOBAL_TIMEZONE_PARAMETER_WITH_OUTPUT;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.attachBody;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class TimezoneOutputIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void checkCapability() {
        assumeTrue(
            "Timezone in output not available",
            hasCapabilities(adminClient(), List.of(GLOBAL_TIMEZONE_PARAMETER_WITH_OUTPUT.capabilityName()))
        );
    }

    private static final String DATE = "2020-02-29T12:30:30.5008009Z";

    @Before
    public void initIndex() throws IOException {
        Request request = new Request("PUT", "/test");
        request.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "date": {
                    "type": "date"
                  },
                  "date_nanos": {
                    "type": "date_nanos"
                  }
                }
              }
            }
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        request = new Request("POST", "/_bulk?index=test&refresh=true");
        request.setJsonEntity(String.format("""
            {"index": {"_id": "1"}}
            {"date": "%s", "date_nanos": "%s"}
            """, DATE, DATE));
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
    }

    private Response esql(String timezone, String expectedContentType) throws IOException {
        String query = (randomBoolean() ? "FROM test" : String.format("ROW date=\"%s\"::date, date_nanos=\"%s\"::date_nanos", DATE, DATE))
            + "| SORT date ASC | LIMIT 2";

        RestEsqlTestCase.RequestObjectBuilder bodyBuilder = new RestEsqlTestCase.RequestObjectBuilder();
        if (randomBoolean()) {
            // Time_zone parameter
            bodyBuilder.timeZone(timezone);
            bodyBuilder.query(query);
        } else {
            // SET time_zone setting
            bodyBuilder.query("SET time_zone=\"" + timezone + "\";\n" + query);
        }

        Response response = executeEsql(bodyBuilder, expectedContentType);
        assertEquals(expectedContentType, response.getEntity().getContentType().getValue());
        return response;

    }

    private Response executeEsql(RestEsqlTestCase.RequestObjectBuilder bodyBuilder, String expectedContentType) throws IOException {
        // Sync request
        if (randomBoolean()) {
            Request request = new Request("POST", "/_query");
            String mediaType = attachBody(bodyBuilder.build(), request);
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Content-Type", mediaType);
            options.addHeader("Accept", expectedContentType);
            request.setOptions(options);
            return client().performRequest(request);
        }

        // Async request
        Request asyncRequest = new Request("POST", "/_query/async");
        bodyBuilder.keepOnCompletion(true);
        bodyBuilder.waitForCompletion(TimeValue.timeValueSeconds(5));
        String mediaType = attachBody(bodyBuilder.build(), asyncRequest);
        RequestOptions.Builder options = asyncRequest.getOptions().toBuilder();
        options.addHeader("Content-Type", mediaType);
        options.addHeader("Accept", expectedContentType);
        asyncRequest.setOptions(options);
        Response asyncResponse = client().performRequest(asyncRequest);

        assertThat(asyncResponse.getHeader("X-Elasticsearch-Async-Is-Running"), equalTo("?0"));
        var id = asyncResponse.getHeader("X-ElasticSearch-Async-Id");
        assertThat(id, notNullValue());

        // Get async value to ensure the values are also correctly rendered there
        Request getAsyncRequest = new Request("GET", "/_query/async/" + id);
        RequestOptions.Builder getAsyncOptions = getAsyncRequest.getOptions().toBuilder();
        getAsyncOptions.addHeader("Accept", expectedContentType);
        getAsyncRequest.setOptions(getAsyncOptions);
        return client().performRequest(getAsyncRequest);
    }

    private String esqlText(String timezone, String expectedContentType) throws Exception {
        try (var reader = new InputStreamReader(esql(timezone, expectedContentType).getEntity().getContent(), StandardCharsets.UTF_8)) {
            return Streams.copyToString(reader);
        }
    }

    public void testTxt() throws Exception {
        assertThat(esqlText("UTC", "text/plain"), equalTo("""
                      date          |         date_nanos        \s
            ------------------------+----------------------------
            2020-02-29T12:30:30.500Z|2020-02-29T12:30:30.5008009Z
            """));
        assertThat(esqlText("+10:00", "text/plain"), equalTo("""
                        date             |           date_nanos           \s
            -----------------------------+---------------------------------
            2020-02-29T22:30:30.500+10:00|2020-02-29T22:30:30.5008009+10:00
            """));
        assertThat(esqlText("Europe/Madrid", "text/plain"), equalTo("""
                        date             |           date_nanos           \s
            -----------------------------+---------------------------------
            2020-02-29T13:30:30.500+01:00|2020-02-29T13:30:30.5008009+01:00
            """));
    }

    public void testCsv() throws Exception {
        assertThat(esqlText("UTC", "text/csv; charset=utf-8; header=present"), equalTo("""
            date,date_nanos\r
            2020-02-29T12:30:30.500Z,2020-02-29T12:30:30.5008009Z\r
            """));
        assertThat(esqlText("+10:00", "text/csv; charset=utf-8; header=present"), equalTo("""
            date,date_nanos\r
            2020-02-29T22:30:30.500+10:00,2020-02-29T22:30:30.5008009+10:00\r
            """));
        assertThat(esqlText("Europe/Madrid", "text/csv; charset=utf-8; header=present"), equalTo("""
            date,date_nanos\r
            2020-02-29T13:30:30.500+01:00,2020-02-29T13:30:30.5008009+01:00\r
            """));
    }

    public void testTsv() throws Exception {
        assertThat(esqlText("UTC", "text/tab-separated-values; charset=utf-8"), equalTo("""
            date\tdate_nanos
            2020-02-29T12:30:30.500Z\t2020-02-29T12:30:30.5008009Z
            """));
        assertThat(esqlText("+10:00", "text/tab-separated-values; charset=utf-8"), equalTo("""
            date\tdate_nanos
            2020-02-29T22:30:30.500+10:00\t2020-02-29T22:30:30.5008009+10:00
            """));
        assertThat(esqlText("Europe/Madrid", "text/tab-separated-values; charset=utf-8"), equalTo("""
            date\tdate_nanos
            2020-02-29T13:30:30.500+01:00\t2020-02-29T13:30:30.5008009+01:00
            """));
    }

    private Map<String, Object> esqlXContent(String timezone, XContentType expectedContentType) throws Exception {
        var map = entityToMap(esql(timezone, expectedContentType.mediaType()).getEntity(), expectedContentType);
        // Remove async fields, if present
        map.remove("id");
        map.remove("is_running");
        return map;
    }

    public void testXContent() throws Exception {
        for (XContentType xContentType : List.of(XContentType.JSON, XContentType.YAML)) {
            var columnMatcher = matchesList().item(matchesMap().entry("name", "date").entry("type", "date"))
                .item(matchesMap().entry("name", "date_nanos").entry("type", "date_nanos"));

            assertResultMap(
                esqlXContent("UTC", xContentType),
                columnMatcher,
                matchesList().item(List.of("2020-02-29T12:30:30.500Z", "2020-02-29T12:30:30.5008009Z"))
            );
            assertResultMap(
                esqlXContent("+10:00", xContentType),
                columnMatcher,
                matchesList().item(List.of("2020-02-29T22:30:30.500+10:00", "2020-02-29T22:30:30.5008009+10:00"))
            );
            assertResultMap(
                esqlXContent("Europe/Madrid", xContentType),
                columnMatcher,
                matchesList().item(List.of("2020-02-29T13:30:30.500+01:00", "2020-02-29T13:30:30.5008009+01:00"))
            );
        }
    }
}
