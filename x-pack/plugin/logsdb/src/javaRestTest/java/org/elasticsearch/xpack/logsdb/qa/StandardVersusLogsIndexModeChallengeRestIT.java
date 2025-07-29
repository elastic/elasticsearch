/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.datageneration.Template;
import org.elasticsearch.datageneration.fields.leaf.BooleanFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.ByteFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.ConstantKeywordFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.CountedKeywordFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.DateFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.DoubleFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.FloatFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.GeoPointFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.HalfFloatFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.IntegerFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.IpFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.KeywordFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.LongFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.ScaledFloatFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.ShortFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.TextFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.UnsignedLongFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.WildcardFieldDataGenerator;
import org.elasticsearch.datageneration.matchers.MatchResult;
import org.elasticsearch.datageneration.matchers.Matcher;
import org.elasticsearch.datageneration.matchers.source.SourceTransforms;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.similarity.ScriptedSimilarity;
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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

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

    private List<String> getSearchableFields(String type, Map<String, Map<String, Object>> mappingLookup) {
        var fields = new ArrayList<String>();
        for (var e : mappingLookup.entrySet()) {
            var mapping = e.getValue();
            if (mapping != null && type.equals(mapping.get("type"))) {
                boolean isIndexed = (Boolean) mapping.getOrDefault("index", true);
                if (isIndexed) {
                    fields.add(e.getKey());
                }
            }
        }
        return fields;
    }

    @SuppressWarnings("unchecked")
    List<String> getNestedPathPrefixes(String[] path) {
        Map<String, Object> mapping = dataGenerationHelper.mapping().raw();
        mapping = (Map<String, Object>) mapping.get("_doc");
        mapping = (Map<String, Object>) mapping.get("properties");

        var result = new ArrayList<String>();
        for (int i = 0; i < path.length - 1; i++) {
            var field = path[i];
            mapping = (Map<String, Object>) mapping.get(field);
            boolean nested = "nested".equals(mapping.get("type"));
            if (nested) {
                result.add(String.join(".", Arrays.copyOfRange(path, 0, i + 1)));
            }
            mapping = (Map<String, Object>) mapping.get("properties");
        }

        mapping = (Map<String, Object>) mapping.get(path[path.length - 1]);
        assert mapping.containsKey("properties") == false;
        return result;
    }

    private QueryBuilder wrapInNestedQuery(String path, QueryBuilder leafQuery) {
        String[] parts = path.split("\\.");
        List<String> nestedPaths = getNestedPathPrefixes(parts);
        QueryBuilder query = leafQuery;
        for (String nestedPath : nestedPaths.reversed()) {
            query = QueryBuilders.nestedQuery(nestedPath, query, ScoreMode.Max);
        }
        return query;
    }

    private boolean isEnabled(String path) {
        String[] parts = path.split("\\.");
        var mappingLookup = dataGenerationHelper.mapping().lookup();
        for (int i = 0; i < parts.length - 1; i++) {
            var pathToHere = String.join(".", Arrays.copyOfRange(parts, 0, i + 1));
            Map<String, Object> mapping = mappingLookup.get(pathToHere);


            boolean enabled = true;
            if (mapping.containsKey("enabled") && mapping.get("enabled") instanceof Boolean) {
                enabled = (Boolean) mapping.get("enabled");
            }
            if (mapping.containsKey("enabled") && mapping.get("enabled") instanceof String) {
                enabled = Boolean.parseBoolean((String) mapping.get("enabled"));
            }

            if (enabled == false) {
                return false;
            }
        }
        return true;
    }

    public void testPhraseQuery() throws IOException {
        int numberOfDocuments = ESTestCase.randomIntBetween(20, 80);
        final List<XContentBuilder> documents = generateDocuments(numberOfDocuments);

        var mappingLookup = dataGenerationHelper.mapping().lookup();
        var fieldsOfType = getSearchableFields("text", mappingLookup);

        if (fieldsOfType.isEmpty()) {
            return;
        }

        var path = randomFrom(fieldsOfType);

        XContentBuilder doc = randomFrom(documents);
        final Map<String, Object> document = XContentHelper.convertToMap(XContentType.JSON.xContent(), Strings.toString(doc), true);
        var normalized = SourceTransforms.normalize(document, mappingLookup);
        List<Object> values = normalized.get(path);
        if (values == null || values.isEmpty()) {
            return;
        }
        String needle = (String) randomFrom(values);
        var tokens = Arrays.asList(needle.split("[^a-zA-Z0-9]"));

        if (tokens.isEmpty()) {
            return;
        }

        int low = ESTestCase.randomIntBetween(0, tokens.size() - 1);
        int hi = ESTestCase.randomIntBetween(low+1, tokens.size());
        var phrase = String.join(" ", tokens.subList(low, hi));
        System.out.println("phrase: " + phrase);

        indexDocuments(documents);

        QueryBuilder queryBuilder = wrapInNestedQuery(path, QueryBuilders.matchPhraseQuery(path, phrase));
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(queryBuilder)
            .size(numberOfDocuments);

        final MatchResult matchResult = Matcher.matchSource()
            .mappings(dataGenerationHelper.mapping().lookup(), getContenderMappings(), getBaselineMappings())
            .settings(getContenderSettings(), getBaselineSettings())
            .expected(getQueryHits(queryBaseline(searchSourceBuilder)))
            .ignoringSort(true)
            .isEqualTo(getQueryHits(queryContender(searchSourceBuilder)));
        assertTrue(matchResult.getMessage(), matchResult.isMatch());
    }

    private static QueryBuilder phraseQuery(String path, Object value) {
        String needle = (String) value;
        var tokens = Arrays.asList(needle.split("[^a-zA-Z0-9]"));

        if (tokens.isEmpty()) {
            return null;
        }

        int low = ESTestCase.randomIntBetween(0, tokens.size() - 1);
        int hi = ESTestCase.randomIntBetween(low+1, tokens.size());
        var phrase = String.join(" ", tokens.subList(low, hi));

        return QueryBuilders.matchPhraseQuery(path, phrase);
    }

    private static List<QueryBuilder> buildLeafQueries(String type, String path, Object value, Map<String, Object> mapping) {
        FieldType fieldType = FieldType.tryParse(type);
        if (fieldType == null) {
            return List.of();
        }
        return switch (fieldType) {
            case KEYWORD ->  {
                var ignoreAbove = (Integer) mapping.getOrDefault("ignore_above", Integer.MAX_VALUE);
                yield ignoreAbove >= ((String) value).length() ? List.of(QueryBuilders.termQuery(path, value)) : List.of();
            }
            case TEXT -> {
                List<QueryBuilder> result = new ArrayList<>(List.of(QueryBuilders.matchQuery(path, value)));
                var phrase = phraseQuery(path, value);
                if (phrase != null) {
                    result.add(phrase);
                }
                yield result;
            }
            case WILDCARD -> {
                var ignoreAbove = (Integer) mapping.getOrDefault("ignore_above", Integer.MAX_VALUE);
                if (ignoreAbove >= ((String) value).length()) {
                    yield List.of(
                        QueryBuilders.termQuery(path, value),
                        QueryBuilders.wildcardQuery(path, value + "*")
                    );
                }
                yield List.of();
            }
            default -> List.of();
        };
    }

    public void testRandomQueries() throws IOException {
        int numberOfDocuments = ESTestCase.randomIntBetween(20, 80);
        final List<XContentBuilder> documents = generateDocuments(numberOfDocuments);
        var mappingLookup = dataGenerationHelper.mapping().lookup();
        final List<Map<String, List<Object>>> docsNormalized = documents.stream().map(d -> {
            var document = XContentHelper.convertToMap(XContentType.JSON.xContent(), Strings.toString(d), true);
            return SourceTransforms.normalize(document, mappingLookup);
        }).toList();

        indexDocuments(documents);

        for (var e : mappingLookup.entrySet()) {
            var path = e.getKey();
            var mapping = e.getValue();

            // This test cannot handle fields with periods in name
            if (path.equals("host.name")) {
                continue;
            }
            if (mapping == null || isEnabled(path) == false) {
                continue;
            }
            boolean isIndexed = (Boolean) mapping.getOrDefault("index", true);
            if (isIndexed == false) {
                continue;
            }
            var docsWithFields = docsNormalized.stream().filter(d -> d.containsKey(path)).toList();
            if (docsWithFields.isEmpty()) {
                continue;
            }

            var doc = randomFrom(docsWithFields);
            List<Object> values = doc.get(path);
            if (values == null) {
                continue;
            }
            List<Object> valuesNonNull = values.stream().filter(Objects::nonNull).toList();
            if (valuesNonNull.isEmpty()) {
                continue;
            }

            Object needle = randomFrom(valuesNonNull);

            var type = (String) mapping.get("type");
            var leafQueries = buildLeafQueries(type, path, needle, mapping).stream().filter(Objects::nonNull).toList();

            for (var leafQuery : leafQueries) {
                QueryBuilder queryBuilder = wrapInNestedQuery(path, leafQuery);
                final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(queryBuilder)
                    .size(numberOfDocuments);

                logger.info("Querying for field [{}] with value [{}]", path, needle);
                final MatchResult matchResult = Matcher.matchSource()
                    .mappings(dataGenerationHelper.mapping().lookup(), getContenderMappings(), getBaselineMappings())
                    .settings(getContenderSettings(), getBaselineSettings())
                    .expected(getQueryHits(queryBaseline(searchSourceBuilder)))
                    .ignoringSort(true)
                    .isEqualTo(getQueryHits(queryContender(searchSourceBuilder)));
                assertTrue(matchResult.getMessage(), matchResult.isMatch());
            }
        }
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
    private List<Map<String, Object>> hits(QueryBuilder query) throws IOException {
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query)
            .size(10);

        Response response = queryBaseline(searchSourceBuilder);

        final Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), response.getEntity().getContent(), true);
        final Map<String, Object> hitsMap = (Map<String, Object>) map.get("hits");
        return (List<Map<String, Object>>) hitsMap.get("hits");
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
