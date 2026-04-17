/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datageneration.DataGeneratorSpecification;
import org.elasticsearch.datageneration.DocumentGenerator;
import org.elasticsearch.datageneration.Mapping;
import org.elasticsearch.datageneration.MappingGenerator;
import org.elasticsearch.datageneration.Template;
import org.elasticsearch.datageneration.TemplateGenerator;
import org.elasticsearch.datageneration.datasource.DataSource;
import org.elasticsearch.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.datageneration.datasource.DefaultObjectGenerationHandler;
import org.elasticsearch.datageneration.matchers.MatchResult;
import org.elasticsearch.datageneration.matchers.Matcher;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class FlattenedRollingUpgradeIT extends AbstractLogsdbRollingUpgradeTestCase {
    private static final String INDEX_NAME = "flattened-bwc-test";
    private static final String INDEX_NAME_NO_INDEX = "flattened-bwc-noindex-test";

    /**
     * Indexing a document into a flattened field and retrieving the synthetic source will cause various source changes (empty arrays are
     * dropped, null values are dropped or replaced with the configured null_value, etc.). These changes would cause the source matcher to
     * fail the test, so this class customizes the data generator to avoid these cases.
     * <p>
     * This class does the following:
     * <ul>
     * <li>Ensures all object fields have at least one child field.
     * <li>Disables dynamically-mapped objects and leaf fields
     * <li>Restricts leaf field types to keyword, text, long, or double
     * <li>Disables empty array values
     * <li>Disables null values
     * <li>Disables mapping parameters other than "type"
     * </ul>
     */
    private static final DataSourceHandler FLATTENED_DATA_GENERATOR = new DataSourceHandler() {
        @Override
        public DataSourceResponse.ChildFieldGenerator handle(DataSourceRequest.ChildFieldGenerator request) {
            return new DefaultObjectGenerationHandler.DefaultChildFieldGenerator(request) {
                @Override

                public int generateChildFieldCount() {
                    return randomIntBetween(1, request.specification().maxFieldCountPerLevel());
                }

                @Override
                public boolean generateDynamicSubObject() {
                    return false;
                }

                @Override
                public boolean generateRegularSubObject() {
                    return randomDouble() < 0.20;
                }

                @Override
                public String generateFieldName() {
                    while (true) {
                        var candidate = super.generateFieldName();
                        if (candidate.contains("\0")) {
                            continue;
                        }
                        return candidate;
                    }
                }
            };
        }

        public DataSourceResponse.LeafMappingParametersGenerator handle(DataSourceRequest.LeafMappingParametersGenerator request) {
            return new DataSourceResponse.LeafMappingParametersGenerator(Collections::emptyMap);
        }

        @Override
        public DataSourceResponse.FieldTypeGenerator handle(DataSourceRequest.FieldTypeGenerator request) {
            return new DataSourceResponse.FieldTypeGenerator(() -> {
                var fieldType = randomFrom("keyword", "text", "long", "double");
                return new DataSourceResponse.FieldTypeGenerator.FieldTypeInfo(fieldType);
            });
        }

        @Override
        public DataSourceResponse.DynamicMappingGenerator handle(DataSourceRequest.DynamicMappingGenerator request) {
            return new DataSourceResponse.DynamicMappingGenerator(isObject -> false);
        }

        @Override
        public DataSourceResponse.ArrayWrapper handle(DataSourceRequest.ArrayWrapper ignored) {
            return new DataSourceResponse.ArrayWrapper(values -> () -> {
                if (ESTestCase.randomBoolean()) {
                    var size = ESTestCase.randomIntBetween(1, 5);
                    return IntStream.range(0, size).mapToObj((i) -> values.get()).toList();
                }

                return values.get();
            });
        }

        @Override
        public DataSourceResponse.NullWrapper handle(DataSourceRequest.NullWrapper ignored) {
            return new DataSourceResponse.NullWrapper(Function.identity());
        }

        @Override
        public DataSourceResponse.ObjectArrayGenerator handle(DataSourceRequest.ObjectArrayGenerator ignored) {
            return new DataSourceResponse.ObjectArrayGenerator(() -> {
                if (ESTestCase.randomBoolean()) {
                    return Optional.of(randomIntBetween(1, 5));
                }
                return Optional.empty();
            });
        }
    };

    private void createIndex(String indexName, Settings settings, boolean index) throws IOException {
        var mappings = XContentFactory.jsonBuilder()
            .startObject()
            .field("dynamic", "strict")
            .startObject("properties")
            .startObject("data")
            .field("type", "flattened")
            .field("index", index)
            .endObject()
            .endObject()
            .endObject();

        createIndex(indexName, settings, Strings.toString(mappings));
    }

    private record FlattenedData(Template template, Mapping mapping, Map<String, Object> document) {}

    private List<FlattenedData> generateFlattenedData(DataGeneratorSpecification spec, int docCount) {
        TemplateGenerator templateGenerator = new TemplateGenerator(spec);
        MappingGenerator mappingGenerator = new MappingGenerator(spec);
        DocumentGenerator documentGenerator = new DocumentGenerator(spec);

        List<FlattenedData> docs = new ArrayList<>(docCount);
        for (int i = 0; i < docCount; i++) {
            var template = templateGenerator.generate();
            var mapping = mappingGenerator.generate(template);
            var document = documentGenerator.generate(template, mapping);
            docs.add(new FlattenedData(template, mapping, document));
        }
        return docs;
    }

    private void indexDocuments(String indexName, List<FlattenedData> flattenedData, int firstId) throws IOException {
        var bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
        StringBuilder requestBody = new StringBuilder();
        int id = firstId;
        for (var data : flattenedData) {
            requestBody.append("{\"index\":{\"_id\":" + (id++) + "}}");
            requestBody.append('\n');
            requestBody.append(
                Strings.toString(XContentFactory.jsonBuilder().startObject().field("data").map(data.document()).endObject())
            );
            requestBody.append('\n');
        }
        bulkRequest.setJsonEntity(requestBody.toString());
        bulkRequest.addParameter("refresh", "true");

        var response = client().performRequest(bulkRequest);
        assertOK(response);

        var responseBody = entityAsMap(response);
        assertThat("errors in response:\n " + responseBody, responseBody.get("errors"), equalTo(false));

        ensureGreen(indexName);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> search(String indexName, int size) throws IOException {
        Request searchRequest = new Request("GET", "/" + indexName + "/_search");

        assert size <= 500;
        searchRequest.setJsonEntity("""
            {
                "query": { "match_all": {} },
                "size": 500
            }""");

        Response response = client().performRequest(searchRequest);
        assertOK(response);

        Map<String, Object> responseMap = entityAsMap(response);
        Integer totalCount = ObjectPath.evaluate(responseMap, "hits.total.value");
        assertThat(totalCount, equalTo(size));

        return ((List<Map<String, Object>>) ObjectPath.evaluate(responseMap, "hits.hits")).stream()
            .sorted(Comparator.comparingInt(map -> Integer.parseInt((String) map.get("_id"))))
            .map(map -> (Map<String, Object>) map.get("_source"))
            .map(source -> (Map<String, Object>) source.get("data"))
            .toList();
    }

    private void compareDocuments(Settings.Builder settings, List<FlattenedData> expectedData, List<Map<String, Object>> actualData)
        throws IOException {
        assertEquals(expectedData.size(), actualData.size());

        for (int i = 0; i < expectedData.size(); i++) {
            FlattenedData flattenedData = expectedData.get(i);

            var xcontentMappings = XContentFactory.jsonBuilder().map(flattenedData.mapping().raw());

            assertNotNull("null doc at " + i, actualData.get(i));

            final MatchResult matchResult = Matcher.matchSource()
                .mappings(flattenedData.mapping().lookup(), xcontentMappings, xcontentMappings)
                .settings(settings, settings)
                .expected(List.of(flattenedData.document()))
                .ignoringSort(true)
                .isEqualTo(List.of(actualData.get(i)));
            assertTrue("[Document " + i + "], " + matchResult.getMessage(), matchResult.isMatch());
        }
    }

    private void verifyExistsQuery(String indexName, int expectedCount) throws IOException {
        Request request = new Request("GET", "/" + indexName + "/_search");
        request.setJsonEntity("""
            {
                "query": { "exists": { "field": "data" } },
                "size": 0
            }""");
        Response response = client().performRequest(request);
        assertOK(response);
        Map<String, Object> responseMap = entityAsMap(response);
        Integer totalCount = ObjectPath.evaluate(responseMap, "hits.total.value");
        assertThat("exists query on 'data' field", totalCount, equalTo(expectedCount));
    }

    @SuppressWarnings("unchecked")
    private void verifyTermsAggregation(String indexName, List<FlattenedData> indexedData) throws IOException {
        Map<String, Integer> expectedBuckets = computeExpectedTermBuckets(indexedData);

        Request request = new Request("GET", "/" + indexName + "/_search");
        request.setJsonEntity("""
            {
                "size": 0,
                "aggs": {
                    "data_terms": {
                        "terms": { "field": "data", "size": 10000 }
                    }
                }
            }""");
        Response response = client().performRequest(request);
        assertOK(response);
        Map<String, Object> responseMap = entityAsMap(response);

        List<Map<String, Object>> buckets = ObjectPath.evaluate(responseMap, "aggregations.data_terms.buckets");
        assertNotNull("terms aggregation returned null buckets", buckets);

        Map<String, Integer> actualBuckets = new HashMap<>();
        for (Map<String, Object> bucket : buckets) {
            actualBuckets.put((String) bucket.get("key"), (int) bucket.get("doc_count"));
        }

        assertThat("terms aggregation bucket count", actualBuckets.size(), equalTo(expectedBuckets.size()));
        for (var entry : expectedBuckets.entrySet()) {
            assertThat("doc_count for term [" + entry.getKey() + "]", actualBuckets.get(entry.getKey()), equalTo(entry.getValue()));
        }
    }

    private static Map<String, Integer> computeExpectedTermBuckets(List<FlattenedData> indexedData) {
        Map<String, Integer> buckets = new HashMap<>();
        for (FlattenedData data : indexedData) {
            Set<String> leafValues = new HashSet<>();
            collectLeafValues(data.document(), leafValues);
            for (String value : leafValues) {
                buckets.merge(value, 1, Integer::sum);
            }
        }
        return buckets;
    }

    @SuppressWarnings("unchecked")
    private static void collectLeafValues(Object value, Set<String> result) {
        if (value instanceof Map<?, ?> map) {
            for (Object v : map.values()) {
                collectLeafValues(v, result);
            }
        } else if (value instanceof Collection<?> list) {
            for (Object item : list) {
                collectLeafValues(item, result);
            }
        } else if (value != null) {
            result.add(value.toString());
        }
    }

    @SuppressWarnings("unchecked")
    private void verifySortByFlattenedField(String indexName, List<FlattenedData> indexedData) throws IOException {
        Request request = new Request("GET", "/" + indexName + "/_search");
        request.setJsonEntity("""
            {
                "size": 500,
                "sort": [ { "data": { "order": "asc" } } ]
            }""");
        Response response = client().performRequest(request);
        assertOK(response);
        Map<String, Object> responseMap = entityAsMap(response);
        Integer totalCount = ObjectPath.evaluate(responseMap, "hits.total.value");
        assertThat("sort result count", totalCount, equalTo(indexedData.size()));

        List<Map<String, Object>> hits = ObjectPath.evaluate(responseMap, "hits.hits");
        assertNotNull("sort returned null hits", hits);

        BytesRef previousSortValue = null;
        for (Map<String, Object> hit : hits) {
            List<Object> sortValues = (List<Object>) hit.get("sort");
            assertNotNull("hit missing sort values", sortValues);
            BytesRef sortValue = new BytesRef((String) sortValues.get(0));
            if (previousSortValue != null) {
                assertTrue(
                    "sort values not in ascending order: [" + previousSortValue.utf8ToString() + "] > [" + sortValue.utf8ToString() + "]",
                    previousSortValue.compareTo(sortValue) <= 0
                );
            }
            previousSortValue = sortValue;
        }
    }

    private void indexDocumentsAndVerifyResults(
        String indexName,
        DataGeneratorSpecification spec,
        Settings.Builder settings,
        List<FlattenedData> indexedData
    ) throws IOException {
        var newDocs = generateFlattenedData(spec, 8);
        indexDocuments(indexName, newDocs, indexedData.size());
        indexedData.addAll(newDocs);

        var actualDocs = search(indexName, indexedData.size());
        compareDocuments(settings, indexedData, actualDocs);

        verifyExistsQuery(indexName, indexedData.size());
        verifyTermsAggregation(indexName, indexedData);
        verifySortByFlattenedField(indexName, indexedData);
    }

    public void testIndexing() throws IOException {
        Settings.Builder settings = Settings.builder().put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "synthetic");

        if (oldClusterHasFeature("gte_v9.3.0")) {
            settings = settings.put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING.getKey(), true);
        }

        createIndex(INDEX_NAME, settings.build(), true);

        DataSource dataSource = new DataSource(List.of(FLATTENED_DATA_GENERATOR));
        DataGeneratorSpecification spec = new DataGeneratorSpecification(dataSource, 4, 4, 0, false, Collections.emptyList());

        List<FlattenedData> indexedData = new ArrayList<>();
        indexDocumentsAndVerifyResults(INDEX_NAME, spec, settings, indexedData);

        Settings.Builder finalSettings = settings;
        clusterRollingUpgrade(index -> { indexDocumentsAndVerifyResults(INDEX_NAME, spec, finalSettings, indexedData); });
    }

    public void testIndexingWithIndexFalse() throws IOException {
        Settings.Builder settings = Settings.builder().put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "synthetic");

        if (oldClusterHasFeature("gte_v9.3.0")) {
            settings = settings.put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING.getKey(), true);
        }

        createIndex(INDEX_NAME_NO_INDEX, settings.build(), false);

        DataSource dataSource = new DataSource(List.of(FLATTENED_DATA_GENERATOR));
        DataGeneratorSpecification spec = new DataGeneratorSpecification(dataSource, 4, 4, 0, false, Collections.emptyList());

        List<FlattenedData> indexedData = new ArrayList<>();
        indexDocumentsAndVerifyResults(INDEX_NAME_NO_INDEX, spec, settings, indexedData);

        Settings.Builder finalSettings = settings;
        clusterRollingUpgrade(index -> { indexDocumentsAndVerifyResults(INDEX_NAME_NO_INDEX, spec, finalSettings, indexedData); });
    }

}
