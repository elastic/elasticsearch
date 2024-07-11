/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb.qa;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.logsdb.qa.exceptions.MatcherException;
import org.elasticsearch.datastreams.logsdb.qa.matchers.Matcher;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;
import org.junit.ClassRule;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StandardVersusLogsIndexModeChallengeRestIT extends AbstractChallengeRestTest {

    @ClassRule()
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .module("constant-keyword")
        .module("data-streams")
        .module("mapper-extras")
        .module("x-pack-aggregate-metric")
        .module("x-pack-stack")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("cluster.logsdb.enabled", "true")
        .build();

    public StandardVersusLogsIndexModeChallengeRestIT() {
        super("logs-apache-baseline", "logs-apache-contender", "baseline-template", "contender-template", 99, 99);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    public void baselineMappings(XContentBuilder builder) throws IOException {
        mappings(builder);
    }

    @Override
    public void contenderMappings(XContentBuilder builder) throws IOException {
        mappings(builder);
    }

    private static void mappings(final XContentBuilder builder) throws IOException {
        builder.field("subobjects", false);
        if (randomBoolean()) {
            builder.startObject("properties")

                .startObject("@timestamp")
                .field("type", "date")
                .endObject()

                .startObject("host.name")
                .field("type", "keyword")
                .field("ignore_above", randomIntBetween(1000, 1200))
                .endObject()

                .startObject("message")
                .field("type", "keyword")
                .field("ignore_above", randomIntBetween(1000, 1200))
                .endObject()

                .startObject("method")
                .field("type", "keyword")
                .field("ignore_above", randomIntBetween(1000, 1200))
                .endObject()

                .startObject("memory_usage_bytes")
                .field("type", "long")
                .field("ignore_malformed", randomBoolean())
                .endObject()

                .endObject();
        }
    }

    private static void settings(final Settings.Builder settings) {
        if (randomBoolean()) {
            settings.put("index.number_of_shards", randomIntBetween(2, 5));
        }
        if (randomBoolean()) {
            settings.put("index.number_of_replicas", randomIntBetween(1, 3));
        }
    }

    @Override
    public void contenderSettings(Settings.Builder builder) {
        builder.put("index.mode", "logs");
        settings(builder);
    }

    @Override
    public void baselineSettings(Settings.Builder builder) {
        settings(builder);
    }

    @Override
    public void beforeStart() throws Exception {
        waitForLogs(client());
    }

    protected static void waitForLogs(RestClient client) throws Exception {
        assertBusy(() -> {
            try {
                final Request request = new Request("GET", "_index_template/logs");
                assertOK(client.performRequest(request));
            } catch (ResponseException e) {
                fail(e.getMessage());
            }
        });
    }

    @SuppressWarnings("unchecked")
    public void testMatchAllQuery() throws IOException, MatcherException {
        final List<XContentBuilder> documents = new ArrayList<>();
        int numberOfDocuments = ESTestCase.randomIntBetween(100, 200);
        for (int i = 0; i < numberOfDocuments; i++) {
            documents.add(generateDocument(Instant.now().plus(i, ChronoUnit.SECONDS)));
        }

        final Tuple<Response, Response> tuple = indexDocuments(() -> documents, () -> documents);
        assertThat(tuple.v1().getStatusLine().getStatusCode(), Matchers.equalTo(RestStatus.OK.getStatus()));
        assertThat(tuple.v2().getStatusLine().getStatusCode(), Matchers.equalTo(RestStatus.OK.getStatus()));

        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .size(numberOfDocuments);

        Matcher.mappings(getContenderMappings(), getBaselineMappings())
            .settings(getContenderSettings(), getBaselineSettings())
            .actual(getResponseSourceAsMap(queryContender(searchSourceBuilder)))
            .expected(getResponseSourceAsMap(queryBaseline(searchSourceBuilder)))
            .ignoreSorting(true)
            .isEqual();
    }

    public void testTermsQuery() throws IOException, MatcherException {
        final List<XContentBuilder> documents = new ArrayList<>();
        int numberOfDocuments = randomIntBetween(100, 200);
        for (int i = 0; i < numberOfDocuments; i++) {
            final String method = randomFrom("put", "post", "get");
            documents.add(generateDocument(Instant.now().plus(i, ChronoUnit.SECONDS)));
        }

        final Tuple<Response, Response> tuple = indexDocuments(() -> documents, () -> documents);
        assertThat(tuple.v1().getStatusLine().getStatusCode(), Matchers.equalTo(RestStatus.OK.getStatus()));
        assertThat(tuple.v2().getStatusLine().getStatusCode(), Matchers.equalTo(RestStatus.OK.getStatus()));

        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.termQuery("method", "put"))
            .size(numberOfDocuments);

        Matcher.mappings(getContenderMappings(), getBaselineMappings())
            .settings(getContenderSettings(), getBaselineSettings())
            .actual(getResponseSourceAsMap(queryContender(searchSourceBuilder)))
            .expected(getResponseSourceAsMap(queryBaseline(searchSourceBuilder)))
            .ignoreSorting(true)
            .isEqual();
    }

    public void testHistogramAggregation() throws IOException, MatcherException {
        final List<XContentBuilder> documents = new ArrayList<>();
        int numberOfDocuments = randomIntBetween(100, 200);
        for (int i = 0; i < numberOfDocuments; i++) {
            documents.add(generateDocument(Instant.now().plus(i, ChronoUnit.SECONDS)));
        }

        final Tuple<Response, Response> tuple = indexDocuments(() -> documents, () -> documents);
        assertThat(tuple.v1().getStatusLine().getStatusCode(), Matchers.equalTo(RestStatus.OK.getStatus()));
        assertThat(tuple.v2().getStatusLine().getStatusCode(), Matchers.equalTo(RestStatus.OK.getStatus()));

        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .size(numberOfDocuments)
            .aggregation(new HistogramAggregationBuilder("agg").field("memory_usage_bytes").interval(100.0D));

        Matcher.mappings(getContenderMappings(), getBaselineMappings())
            .settings(getContenderSettings(), getBaselineSettings())
            .actual(getResponseSourceAsMap(queryContender(searchSourceBuilder)))
            .expected(getResponseSourceAsMap(queryBaseline(searchSourceBuilder)))
            .ignoreSorting(true)
            .isEqual();
    }

    public void testTermsAggregation() throws IOException, MatcherException {
        final List<XContentBuilder> documents = new ArrayList<>();
        int numberOfDocuments = randomIntBetween(100, 200);
        for (int i = 0; i < numberOfDocuments; i++) {
            documents.add(generateDocument(Instant.now().plus(i, ChronoUnit.SECONDS)));
        }

        final Tuple<Response, Response> tuple = indexDocuments(() -> documents, () -> documents);
        assertThat(tuple.v1().getStatusLine().getStatusCode(), Matchers.equalTo(RestStatus.OK.getStatus()));
        assertThat(tuple.v2().getStatusLine().getStatusCode(), Matchers.equalTo(RestStatus.OK.getStatus()));

        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .size(numberOfDocuments)
            .size(0)
            .aggregation(new TermsAggregationBuilder("agg").field("host.name"));

        Matcher.mappings(getContenderMappings(), getBaselineMappings())
            .settings(getContenderSettings(), getBaselineSettings())
            .actual(getResponseSourceAsMap(queryContender(searchSourceBuilder)))
            .expected(getResponseSourceAsMap(queryBaseline(searchSourceBuilder)))
            .ignoreSorting(true)
            .isEqual();
    }

    public void testDateHistogramAggregation() throws IOException, MatcherException {
        final List<XContentBuilder> documents = new ArrayList<>();
        int numberOfDocuments = randomIntBetween(100, 200);
        for (int i = 0; i < numberOfDocuments; i++) {
            documents.add(generateDocument(Instant.now().plus(i, ChronoUnit.SECONDS)));
        }

        final Tuple<Response, Response> tuple = indexDocuments(() -> documents, () -> documents);
        assertThat(tuple.v1().getStatusLine().getStatusCode(), Matchers.equalTo(RestStatus.OK.getStatus()));
        assertThat(tuple.v2().getStatusLine().getStatusCode(), Matchers.equalTo(RestStatus.OK.getStatus()));

        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .aggregation(AggregationBuilders.dateHistogram("agg").field("@timestamp").calendarInterval(DateHistogramInterval.SECOND))
            .size(0);

        Matcher.mappings(getContenderMappings(), getBaselineMappings())
            .settings(getContenderSettings(), getBaselineSettings())
            .actual(getResponseSourceAsMap(queryContender(searchSourceBuilder)))
            .expected(getResponseSourceAsMap(queryBaseline(searchSourceBuilder)))
            .ignoreSorting(true)
            .isEqual();
    }

    private static XContentBuilder generateDocument(final Instant timestamp) throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .field("@timestamp", DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(timestamp))
            .field("host.name", randomFrom("foo", "bar", "baz"))
            .field("message", randomFrom("a message", "another message", "still another message", "one more message"))
            .field("method", randomFrom("put", "post", "get"))
            .field("memory_usage_bytes", randomLongBetween(1000, 2000))
            .endObject();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> getResponseSourceAsMap(final Response response) throws IOException {
        final Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), response.getEntity().getContent(), true);
        final Map<String, Object> hitsMap = (Map<String, Object>) map.get("hits");
        final List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hitsMap.get("hits");

        return hitsList.stream().map(hit -> (Map<String, Object>) hit.get("_source")).toList();
    }
}
