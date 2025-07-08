/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.datageneration.matchers.MatchResult;
import org.elasticsearch.datageneration.matchers.Matcher;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Challenge test (see {@link BulkStaticMappingChallengeRestIT}) that uses randomly generated
 * mapping and documents in order to cover more code paths and permutations.
 */
public abstract class StandardVersusLogsIndexModeChallengeRestIT extends AbstractChallengeRestTest {
    protected final boolean fullyDynamicMapping = randomBoolean();
    private final boolean useCustomSortConfig = fullyDynamicMapping == false && randomBoolean();
    private final boolean routeOnSortFields = useCustomSortConfig && randomBoolean();
    private final int numShards = randomBoolean() ? randomIntBetween(2, 4) : 0;
    private final int numReplicas = randomBoolean() ? randomIntBetween(1, 3) : 0;
    protected final DataGenerationHelper dataGenerationHelper;

    public StandardVersusLogsIndexModeChallengeRestIT() {
        this(new DataGenerationHelper());
    }

    protected StandardVersusLogsIndexModeChallengeRestIT(DataGenerationHelper dataGenerationHelper) {
        super("standard-apache-baseline", "logs-apache-contender", "baseline-template", "contender-template", 101, 101);
        this.dataGenerationHelper = dataGenerationHelper;
    }

    @Override
    public void baselineMappings(XContentBuilder builder) throws IOException {
        dataGenerationHelper.writeStandardMapping(builder);
    }

    @Override
    public void contenderMappings(XContentBuilder builder) throws IOException {
        dataGenerationHelper.writeLogsDbMapping(builder);
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
        if (useCustomSortConfig) {
            builder.putList("index.sort.field", "host.name", "method", "@timestamp");
            builder.putList("index.sort.order", "asc", "asc", "desc");
            if (routeOnSortFields) {
                builder.put("index.logsdb.route_on_sort_fields", true);
            }
        }
        dataGenerationHelper.logsDbSettings(builder);
    }

    @Override
    public void baselineSettings(Settings.Builder builder) {}

    @Override
    public void beforeStart() throws Exception {
        waitForLogs(client());
    }

    protected boolean autoGenerateId() {
        return routeOnSortFields;
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

    public void testMatchAllQuery() throws IOException {
        int numberOfDocuments = ESTestCase.randomIntBetween(20, 80);
        final List<XContentBuilder> documents = generateDocuments(numberOfDocuments);

        indexDocuments(documents);

        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .size(numberOfDocuments);

        final MatchResult matchResult = Matcher.matchSource()
            .mappings(dataGenerationHelper.mapping().lookup(), getContenderMappings(), getBaselineMappings())
            .settings(getContenderSettings(), getBaselineSettings())
            .expected(getQueryHits(queryBaseline(searchSourceBuilder)))
            .ignoringSort(true)
            .isEqualTo(getQueryHits(queryContender(searchSourceBuilder)));
        assertTrue(matchResult.getMessage(), matchResult.isMatch());
    }

    public void testTermsQuery() throws IOException {
        int numberOfDocuments = ESTestCase.randomIntBetween(20, 80);
        final List<XContentBuilder> documents = generateDocuments(numberOfDocuments);

        indexDocuments(documents);

        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.termQuery("method", "put"))
            .size(numberOfDocuments);

        final MatchResult matchResult = Matcher.matchSource()
            .mappings(dataGenerationHelper.mapping().lookup(), getContenderMappings(), getBaselineMappings())
            .settings(getContenderSettings(), getBaselineSettings())
            .expected(getQueryHits(queryBaseline(searchSourceBuilder)))
            .ignoringSort(true)
            .isEqualTo(getQueryHits(queryContender(searchSourceBuilder)));
        assertTrue(matchResult.getMessage(), matchResult.isMatch());
    }

    public void testHistogramAggregation() throws IOException {
        int numberOfDocuments = ESTestCase.randomIntBetween(20, 80);
        final List<XContentBuilder> documents = generateDocuments(numberOfDocuments);

        indexDocuments(documents);

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
        int numberOfDocuments = ESTestCase.randomIntBetween(20, 80);
        final List<XContentBuilder> documents = generateDocuments(numberOfDocuments);

        indexDocuments(documents);

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
        int numberOfDocuments = ESTestCase.randomIntBetween(20, 80);
        final List<XContentBuilder> documents = generateDocuments(numberOfDocuments);

        indexDocuments(documents);

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

    public void testEsqlSource() throws IOException {
        int numberOfDocuments = ESTestCase.randomIntBetween(20, 80);
        final List<XContentBuilder> documents = generateDocuments(numberOfDocuments);

        indexDocuments(documents);

        final String query = "FROM $index METADATA _source, _id | KEEP _source, _id | LIMIT " + numberOfDocuments;
        final MatchResult matchResult = Matcher.matchSource()
            .mappings(dataGenerationHelper.mapping().lookup(), getContenderMappings(), getBaselineMappings())
            .settings(getContenderSettings(), getBaselineSettings())
            .expected(getEsqlSourceResults(esqlBaseline(query)))
            .ignoringSort(true)
            .isEqualTo(getEsqlSourceResults(esqlContender(query)));
        assertTrue(matchResult.getMessage(), matchResult.isMatch());
    }

    public void testEsqlTermsAggregation() throws IOException {
        int numberOfDocuments = ESTestCase.randomIntBetween(20, 80);
        final List<XContentBuilder> documents = generateDocuments(numberOfDocuments);

        indexDocuments(documents);

        final String query = "FROM $index | STATS count(*) BY host.name | SORT host.name | LIMIT " + numberOfDocuments;
        final MatchResult matchResult = Matcher.mappings(getContenderMappings(), getBaselineMappings())
            .settings(getContenderSettings(), getBaselineSettings())
            .expected(getEsqlStatsResults(esqlBaseline(query)))
            .ignoringSort(true)
            .isEqualTo(getEsqlStatsResults(esqlContender(query)));
        assertTrue(matchResult.getMessage(), matchResult.isMatch());
    }

    public void testEsqlTermsAggregationByMethod() throws IOException {
        int numberOfDocuments = ESTestCase.randomIntBetween(20, 80);
        final List<XContentBuilder> documents = generateDocuments(numberOfDocuments);

        indexDocuments(documents);

        final String query = "FROM $index | STATS count(*) BY method | SORT method | LIMIT " + numberOfDocuments;
        final MatchResult matchResult = Matcher.mappings(getContenderMappings(), getBaselineMappings())
            .settings(getContenderSettings(), getBaselineSettings())
            .expected(getEsqlStatsResults(esqlBaseline(query)))
            .ignoringSort(true)
            .isEqualTo(getEsqlStatsResults(esqlContender(query)));
        assertTrue(matchResult.getMessage(), matchResult.isMatch());
    }

    public void testFieldCaps() throws IOException {
        int numberOfDocuments = ESTestCase.randomIntBetween(20, 50);
        final List<XContentBuilder> documents = generateDocuments(numberOfDocuments);

        indexDocuments(documents);

        final MatchResult matchResult = Matcher.mappings(getContenderMappings(), getBaselineMappings())
            .settings(getContenderSettings(), getBaselineSettings())
            .expected(getFields(fieldCapsBaseline()))
            .ignoringSort(true)
            .isEqualTo(getFields(fieldCapsContender()));
        assertTrue(matchResult.getMessage(), matchResult.isMatch());
    }

    private List<XContentBuilder> generateDocuments(int numberOfDocuments) throws IOException {
        final List<XContentBuilder> documents = new ArrayList<>();
        // This is static in order to be able to identify documents between test runs.
        var startingPoint = ZonedDateTime.of(2024, 1, 1, 10, 0, 0, 0, ZoneId.of("UTC")).toInstant();
        for (int i = 0; i < numberOfDocuments; i++) {
            documents.add(generateDocument(startingPoint.plus(i, ChronoUnit.SECONDS)));
        }

        return documents;
    }

    protected XContentBuilder generateDocument(final Instant timestamp) throws IOException {
        var document = XContentFactory.jsonBuilder();
        dataGenerationHelper.generateDocument(
            document,
            Map.of("@timestamp", DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(timestamp))
        );
        return document;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> getQueryHits(final Response response) throws IOException {
        final Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), response.getEntity().getContent(), true);
        final Map<String, Object> hitsMap = (Map<String, Object>) map.get("hits");

        final List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hitsMap.get("hits");
        assertThat(hitsList.size(), greaterThan(0));

        return hitsList.stream()
            .sorted(Comparator.comparing((Map<String, Object> hit) -> ((String) hit.get("_id"))))
            .map(hit -> (Map<String, Object>) hit.get("_source"))
            .toList();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getFields(final Response response) throws IOException {
        final Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), response.getEntity().getContent(), true);
        final Map<String, Object> fields = (Map<String, Object>) map.get("fields");
        assertThat(fields.size(), greaterThan(0));
        return new TreeMap<>(fields);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> getEsqlSourceResults(final Response response) throws IOException {
        final Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), response.getEntity().getContent(), true);
        final List<List<Object>> values = (List<List<Object>>) map.get("values");
        assertThat(values.size(), greaterThan(0));

        // Results contain a list of [source, id] lists.
        return values.stream()
            .sorted(Comparator.comparing((List<Object> value) -> ((String) value.get(1))))
            .map(value -> (Map<String, Object>) value.get(0))
            .toList();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> getEsqlStatsResults(final Response response) throws IOException {
        final Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), response.getEntity().getContent(), true);
        final List<List<Object>> values = (List<List<Object>>) map.get("values");
        assertThat(values.size(), greaterThan(0));

        // Results contain a list of [agg value, group name] lists.
        return values.stream()
            .sorted(Comparator.comparing((List<Object> value) -> (String) value.get(1)))
            .map(value -> Map.of((String) value.get(1), value.get(0)))
            .toList();
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

    private void indexDocuments(List<XContentBuilder> documents) throws IOException {
        indexDocuments(() -> documents, () -> documents);
    }

    protected final Map<String, Object> performBulkRequest(String json, boolean isBaseline) throws IOException {
        var request = new Request("POST", "/" + (isBaseline ? getBaselineDataStreamName() : getContenderDataStreamName()) + "/_bulk");
        request.setJsonEntity(json);
        request.addParameter("refresh", "true");
        var response = client.performRequest(request);
        assertOK(response);
        var responseBody = entityAsMap(response);
        assertThat(
            "errors in " + (isBaseline ? "baseline" : "contender") + " bulk response:\n " + responseBody,
            responseBody.get("errors"),
            equalTo(false)
        );
        return responseBody;
    }
}
