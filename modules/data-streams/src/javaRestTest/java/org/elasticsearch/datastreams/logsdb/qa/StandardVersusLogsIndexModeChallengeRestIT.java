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
import org.elasticsearch.datastreams.logsdb.qa.matchers.MatchResult;
import org.elasticsearch.datastreams.logsdb.qa.matchers.Matcher;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.logsdb.datageneration.DataGenerator;
import org.elasticsearch.logsdb.datageneration.DataGeneratorSpecification;
import org.elasticsearch.logsdb.datageneration.FieldType;
import org.elasticsearch.logsdb.datageneration.arbitrary.RandomBasedArbitrary;
import org.elasticsearch.logsdb.datageneration.fields.PredefinedField;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class StandardVersusLogsIndexModeChallengeRestIT extends AbstractChallengeRestTest {
    private final DataGenerator dataGenerator;
    private final int numShards = randomBoolean() ? randomIntBetween(2, 5) : 0;
    private final int numReplicas = randomBoolean() ? randomIntBetween(1, 3) : 0;

    public StandardVersusLogsIndexModeChallengeRestIT() {
        super("standard-apache-baseline", "logs-apache-contender", "baseline-template", "contender-template", 101, 101);
        this.dataGenerator = new DataGenerator(
            DataGeneratorSpecification.builder()
                // Nested fields don't work with subobjects: false.
                .withNestedFieldsLimit(0)
                // TODO increase depth of objects
                // Currently matching fails because in synthetic source all fields are flat (given that we have subobjects: false)
                // but stored source is identical to original document which has nested structure.
                .withMaxObjectDepth(0)
                .withArbitrary(new RandomBasedArbitrary() {
                    // TODO enable null values
                    // Matcher does not handle nulls currently
                    @Override
                    public boolean generateNullValue() {
                        return false;
                    }

                    // TODO enable arrays
                    // List matcher currently does not apply matching logic recursively
                    // and equality check fails because arrays are sorted in synthetic source.
                    @Override
                    public boolean generateArrayOfValues() {
                        return false;
                    }
                })
                .withPredefinedFields(List.of(new PredefinedField("host.name", FieldType.KEYWORD)))
                .build()
        );
    }

    @Override
    public void baselineMappings(XContentBuilder builder) throws IOException {
        if (randomBoolean()) {
            dataGenerator.writeMapping(builder);
        } else {
            // We want dynamic mapping, but we need host.name to be a keyword instead of text to support aggregations.
            builder.startObject()
                .startObject("properties")

                .startObject("host.name")
                .field("type", "keyword")
                .field("ignore_above", randomIntBetween(1000, 1200))
                .endObject()

                .endObject()
                .endObject();
        }
    }

    @Override
    public void contenderMappings(XContentBuilder builder) throws IOException {
        if (randomBoolean()) {
            dataGenerator.writeMapping(builder, b -> builder.field("subobjects", false));
        } else {
            // Sometimes we go with full dynamic mapping.
            builder.startObject();
            builder.field("subobjects", false);
            builder.endObject();
        }
    }

    @Override
    public void commonSettings(Settings.Builder builder) {
        if (numShards > 0) {
            builder.put("index.number_of_shards", numShards);
        }
        if (numReplicas > 0) {
            builder.put("index.number_of_replicas", numReplicas);
        }
        builder.put("index.mapping.total_fields.limit", 5000);
    }

    @Override
    public void contenderSettings(Settings.Builder builder) {
        builder.put("index.mode", "logsdb");
    }

    @Override
    public void baselineSettings(Settings.Builder builder) {}

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
    public void testMatchAllQuery() throws IOException {
        final List<XContentBuilder> documents = new ArrayList<>();
        int numberOfDocuments = ESTestCase.randomIntBetween(100, 200);
        for (int i = 0; i < numberOfDocuments; i++) {
            documents.add(generateDocument(Instant.now().plus(i, ChronoUnit.SECONDS)));
        }

        assertDocumentIndexing(documents);

        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .size(numberOfDocuments);

        final MatchResult matchResult = Matcher.mappings(getContenderMappings(), getBaselineMappings())
            .settings(getContenderSettings(), getBaselineSettings())
            .expected(getQueryHits(queryBaseline(searchSourceBuilder)))
            .ignoringSort(true)
            .isEqualTo(getQueryHits(queryContender(searchSourceBuilder)));
        assertTrue(matchResult.getMessage(), matchResult.isMatch());
    }

    public void testTermsQuery() throws IOException {
        final List<XContentBuilder> documents = new ArrayList<>();
        int numberOfDocuments = randomIntBetween(100, 200);
        for (int i = 0; i < numberOfDocuments; i++) {
            documents.add(generateDocument(Instant.now().plus(i, ChronoUnit.SECONDS)));
        }

        assertDocumentIndexing(documents);

        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.termQuery("method", "put"))
            .size(numberOfDocuments);

        final MatchResult matchResult = Matcher.mappings(getContenderMappings(), getBaselineMappings())
            .settings(getContenderSettings(), getBaselineSettings())
            .expected(getQueryHits(queryBaseline(searchSourceBuilder)))
            .ignoringSort(true)
            .isEqualTo(getQueryHits(queryContender(searchSourceBuilder)));
        assertTrue(matchResult.getMessage(), matchResult.isMatch());
    }

    public void testHistogramAggregation() throws IOException {
        final List<XContentBuilder> documents = new ArrayList<>();
        int numberOfDocuments = randomIntBetween(100, 200);
        for (int i = 0; i < numberOfDocuments; i++) {
            documents.add(generateDocument(Instant.now().plus(i, ChronoUnit.SECONDS)));
        }

        assertDocumentIndexing(documents);

        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .size(numberOfDocuments)
            .aggregation(new HistogramAggregationBuilder("agg").field("memory_usage_bytes").interval(100.0D));

        final MatchResult matchResult = Matcher.mappings(getContenderMappings(), getBaselineMappings())
            .settings(getContenderSettings(), getBaselineSettings())
            .expected(getAggregationBuckets(queryBaseline(searchSourceBuilder), "agg"))
            .ignoringSort(true)
            .isEqualTo(getAggregationBuckets(queryContender(searchSourceBuilder), "agg"));
        assertTrue(matchResult.getMessage(), matchResult.isMatch());
    }

    public void testTermsAggregation() throws IOException {
        final List<XContentBuilder> documents = new ArrayList<>();
        int numberOfDocuments = randomIntBetween(100, 200);
        for (int i = 0; i < numberOfDocuments; i++) {
            documents.add(generateDocument(Instant.now().plus(i, ChronoUnit.SECONDS)));
        }

        assertDocumentIndexing(documents);

        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .size(0)
            .aggregation(new TermsAggregationBuilder("agg").field("host.name").size(numberOfDocuments));

        final MatchResult matchResult = Matcher.mappings(getContenderMappings(), getBaselineMappings())
            .settings(getContenderSettings(), getBaselineSettings())
            .expected(getAggregationBuckets(queryBaseline(searchSourceBuilder), "agg"))
            .ignoringSort(true)
            .isEqualTo(getAggregationBuckets(queryContender(searchSourceBuilder), "agg"));
        assertTrue(matchResult.getMessage(), matchResult.isMatch());
    }

    public void testDateHistogramAggregation() throws IOException {
        final List<XContentBuilder> documents = new ArrayList<>();
        int numberOfDocuments = randomIntBetween(100, 200);
        for (int i = 0; i < numberOfDocuments; i++) {
            documents.add(generateDocument(Instant.now().plus(i, ChronoUnit.SECONDS)));
        }

        assertDocumentIndexing(documents);

        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .aggregation(AggregationBuilders.dateHistogram("agg").field("@timestamp").calendarInterval(DateHistogramInterval.SECOND))
            .size(0);

        final MatchResult matchResult = Matcher.mappings(getContenderMappings(), getBaselineMappings())
            .settings(getContenderSettings(), getBaselineSettings())
            .expected(getAggregationBuckets(queryBaseline(searchSourceBuilder), "agg"))
            .ignoringSort(true)
            .isEqualTo(getAggregationBuckets(queryContender(searchSourceBuilder), "agg"));
        assertTrue(matchResult.getMessage(), matchResult.isMatch());
    }

    private XContentBuilder generateDocument(final Instant timestamp) throws IOException {
        var document = XContentFactory.jsonBuilder();
        dataGenerator.generateDocument(document, doc -> {
            doc.field("@timestamp", DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(timestamp));
            // Needed for terms query
            doc.field("method", randomFrom("put", "post", "get"));
            // We can generate this but we would get "too many buckets"
            doc.field("memory_usage_bytes", randomLongBetween(1000, 2000));
        });

        return document;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> getQueryHits(final Response response) throws IOException {
        final Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), response.getEntity().getContent(), true);
        final Map<String, Object> hitsMap = (Map<String, Object>) map.get("hits");

        final List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hitsMap.get("hits");
        assertThat(hitsList.size(), greaterThan(0));

        return hitsList.stream().map(hit -> (Map<String, Object>) hit.get("_source")).toList();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> getAggregationBuckets(final Response response, final String aggName) throws IOException {
        final Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), response.getEntity().getContent(), true);
        final Map<String, Object> aggs = (Map<String, Object>) map.get("aggregations");
        final Map<String, Object> agg = (Map<String, Object>) aggs.get(aggName);

        var buckets = (List<Map<String, Object>>) agg.get("buckets");
        assertThat(buckets.size(), greaterThan(0));

        return buckets;
    }

    private void assertDocumentIndexing(List<XContentBuilder> documents) throws IOException {
        final Tuple<Response, Response> tuple = indexDocuments(() -> documents, () -> documents);

        assertThat(tuple.v1().getStatusLine().getStatusCode(), Matchers.equalTo(RestStatus.OK.getStatus()));
        var baselineResponseBody = entityAsMap(tuple.v1());
        assertThat("errors in baseline bulk response:\n " + baselineResponseBody, baselineResponseBody.get("errors"), equalTo(false));

        assertThat(tuple.v2().getStatusLine().getStatusCode(), Matchers.equalTo(RestStatus.OK.getStatus()));
        var contenderResponseBody = entityAsMap(tuple.v2());
        assertThat("errors in contender bulk response:\n " + contenderResponseBody, contenderResponseBody.get("errors"), equalTo(false));
    }

}
