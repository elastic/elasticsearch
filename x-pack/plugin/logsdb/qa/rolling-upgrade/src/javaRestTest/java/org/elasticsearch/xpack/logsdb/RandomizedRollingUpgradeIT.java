/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.datageneration.DataGeneratorSpecification;
import org.elasticsearch.datageneration.DocumentGenerator;
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.datageneration.Mapping;
import org.elasticsearch.datageneration.MappingGenerator;
import org.elasticsearch.datageneration.Template;
import org.elasticsearch.datageneration.TemplateGenerator;
import org.elasticsearch.datageneration.datasource.ASCIIStringsHandler;
import org.elasticsearch.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.datageneration.datasource.DefaultMappingParametersHandler;
import org.elasticsearch.datageneration.datasource.DefaultObjectGenerationHandler;
import org.elasticsearch.datageneration.datasource.MultifieldAddonHandler;
import org.elasticsearch.datageneration.fields.PredefinedField;
import org.elasticsearch.datageneration.fields.leaf.FlattenedFieldDataGenerator;
import org.elasticsearch.datageneration.matchers.MatchResult;
import org.elasticsearch.datageneration.matchers.Matcher;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class RandomizedRollingUpgradeIT extends AbstractLogsdbRollingUpgradeTestCase {

    private record TestIndexConfig(
        String indexName,
        Template template,
        Settings.Builder settings,
        Mapping mapping,
        List<String> documents
    ) {
        TestIndexConfig(String indexName, Template template, Settings.Builder settings, Mapping mapping) {
            this(indexName, template, settings, mapping, new ArrayList<>());
        }
    }

    private static final int NUM_INDICES = 3;
    private static final int NUM_DOCS = 8;
    private static DocumentGenerator documentGenerator;
    private static TemplateGenerator templateGenerator;
    private static MappingGenerator mappingGenerator;

    @BeforeClass
    public static void setupGenerators() {
        var specification = DataGeneratorSpecification.builder()
            .withMaxObjectDepth(2)
            .withMaxFieldCountPerLevel(6)
            .withPredefinedFields(
                List.of(
                    new PredefinedField.WithGeneratorProvider(
                        "flattened",
                        FieldType.FLATTENED,
                        Map.of("type", "flattened"),
                        FlattenedFieldDataGenerator::new
                    )
                )
            )
            // 9.0.x: do not generate counted_keyword (object array + counted_keyword triggers indexing bug)
            .withDataSourceHandlers(List.of(new DataSourceHandler() {
                @Override
                public DataSourceResponse.FieldTypeGenerator handle(DataSourceRequest.FieldTypeGenerator request) {
                    if (System.getProperty("tests.old_cluster_version", "").startsWith("9.0.") == false) {
                        return null;
                    }
                    var allowed = DefaultObjectGenerationHandler.ALLOWED_FIELD_TYPES.stream()
                        .filter(ft -> ft != FieldType.COUNTED_KEYWORD)
                        .toList();
                    return new DataSourceResponse.FieldTypeGenerator(
                        () -> new DataSourceResponse.FieldTypeGenerator.FieldTypeInfo(ESTestCase.randomFrom(allowed).toString())
                    );
                }
            }))
            .withDataSourceHandlers(List.of(new DefaultMappingParametersHandler() {
                @Override
                protected Object extendedDocValuesParams() {
                    if (oldClusterHasFeature("mapper.keyword.store_high_cardinality_in_binary_doc_values")) {
                        return super.extendedDocValuesParams();
                    }

                    return ESTestCase.randomBoolean();
                }
            }))
            .withDataSourceHandlers(List.of(MultifieldAddonHandler.STRING_TYPE_HANDLER))
            // TODO: Remove ASCIIStringHandlers once the test has stabilized
            .withDataSourceHandlers(List.of(new ASCIIStringsHandler()))
            .build();

        documentGenerator = new DocumentGenerator(specification);
        templateGenerator = new TemplateGenerator(specification);
        mappingGenerator = new MappingGenerator(specification);
    }

    private TestIndexConfig createIndex(String indexName, Settings.Builder settings) throws IOException {
        var template = templateGenerator.generate();
        TestIndexConfig indexConfig = new TestIndexConfig(indexName, template, settings, mappingGenerator.generate(template));

        @SuppressWarnings("unchecked")
        Map<String, Object> mappingRaw = (Map<String, Object>) indexConfig.mapping.raw().get("_doc");
        String mappingStr = Strings.toString(XContentFactory.jsonBuilder().map(mappingRaw));
        logger.info(() -> indexName + " mappings: " + mappingStr);
        createIndex(indexName, settings.build(), mappingStr);

        return indexConfig;
    }

    private void indexAndQueryDocuments(TestIndexConfig indexConfig) throws IOException {
        indexDocuments(indexConfig);
        testQueryAll(indexConfig);
        testEsqlSource(indexConfig);
    }

    @Override
    public String getEnsureGreenTimeout() {
        return "2m";
    }

    private void testIndexing(String indexNameBase, Settings.Builder settings) throws IOException {
        TestIndexConfig[] indexConfigs = new TestIndexConfig[NUM_INDICES];

        for (int i = 0; i < NUM_INDICES; i++) {
            indexConfigs[i] = createIndex(indexNameBase + i, settings);
            indexAndQueryDocuments(indexConfigs[i]);
        }

        int numNodes = Integer.parseInt(System.getProperty("tests.num_nodes", "3"));
        for (int i = 0; i < numNodes; i++) {
            flush(indexNameBase + "*", true);
            upgradeNode(i);
            ensureGreen(indexNameBase + "*");
            for (int j = 0; j < NUM_INDICES; j++) {
                indexAndQueryDocuments(indexConfigs[j]);
            }
        }
    }

    public void testIndexingStandardSource() throws IOException {
        Settings.Builder builder = Settings.builder().put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "stored");
        String indexNameBase = "test-index-standard-";
        testIndexing(indexNameBase, builder);
    }

    public void testIndexingSyntheticSource() throws IOException {
        Settings.Builder builder = Settings.builder().put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "synthetic");
        if (randomBoolean()) {
            builder.put(Mapper.SYNTHETIC_SOURCE_KEEP_INDEX_SETTING.getKey(), "arrays");
        }
        String indexNameBase = "test-index-synthetic-";
        testIndexing(indexNameBase, builder);
    }

    private void indexDocuments(TestIndexConfig indexConfig) throws IOException {
        StringBuilder bulkBuilder = new StringBuilder();
        for (int i = 0; i < NUM_DOCS; ++i) {
            int docId = indexConfig.documents.size();
            Map<String, Object> doc = documentGenerator.generate(indexConfig.template, indexConfig.mapping);
            String docStr = Strings.toString(XContentFactory.jsonBuilder().map(doc));
            indexConfig.documents.add(docStr);
            bulkBuilder.append(Strings.format("{\"create\":{ \"_id\": %d }}\n", docId));
            bulkBuilder.append(docStr).append('\n');
        }

        String jsonBody = bulkBuilder.toString();
        var request = new Request("POST", "/" + indexConfig.indexName + "/_bulk");
        request.setJsonEntity(jsonBody);
        request.addParameter("refresh", "true");
        var response = client().performRequest(request);
        assertOK(response);
        var responseBody = entityAsMap(response);
        assertThat("errors in bulk response:\n " + responseBody, responseBody.get("errors"), equalTo(false));
    }

    private void testQueryAll(TestIndexConfig indexConfig) throws IOException {
        var xcontentMappings = XContentFactory.jsonBuilder().map(indexConfig.mapping().raw());

        var actualSettings = getIndexSettingsAsMap(indexConfig.indexName);
        var actualSettingsBuilder = Settings.builder().loadFromMap(actualSettings);

        var query = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(indexConfig.documents.size());

        var expectedDocs = indexConfig.documents.stream()
            .map(d -> XContentHelper.convertToMap(XContentType.JSON.xContent(), d, true))
            .toList();

        var queryHits = getQueryHits(queryIndex(indexConfig.indexName, query));

        final MatchResult matchResult = Matcher.matchSource()
            .mappings(indexConfig.mapping().lookup(), xcontentMappings, xcontentMappings)
            .settings(actualSettingsBuilder, indexConfig.settings)
            .expected(expectedDocs)
            .ignoringSort(true)
            .isEqualTo(queryHits);
        assertTrue(matchResult.getMessage(), matchResult.isMatch());
    }

    private Response queryIndex(final String indexName, final SearchSourceBuilder search) throws IOException {
        final Request request = new Request("GET", "/" + indexName + "/_search");
        request.setJsonEntity(Strings.toString(search));
        return client().performRequest(request);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getQueryHits(final Response response) throws IOException {
        final Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), response.getEntity().getContent(), true);

        final List<Map<String, Object>> hitsList = (List<Map<String, Object>>) ((Map<String, Object>) map.get("hits")).get("hits");

        assertThat(hitsList.size(), greaterThan(0));

        return hitsList.stream()
            .sorted(Comparator.comparing((Map<String, Object> hit) -> Integer.valueOf((String) hit.get("_id"))))
            .map(hit -> (Map<String, Object>) hit.get("_source"))
            .toList();
    }

    private void testEsqlSource(TestIndexConfig indexConfig) throws IOException {
        var xcontentMappings = XContentFactory.jsonBuilder().map(indexConfig.mapping().raw());

        var actualSettings = getIndexSettingsAsMap(indexConfig.indexName);
        var actualSettingsBuilder = Settings.builder().loadFromMap(actualSettings);

        var expectedDocs = indexConfig.documents.stream()
            .map(d -> XContentHelper.convertToMap(XContentType.JSON.xContent(), d, true))
            .toList();

        final String query = "FROM "
            + indexConfig.indexName
            + " METADATA _source, _id | KEEP _source, _id | LIMIT "
            + indexConfig.documents.size();
        var queryHits = getEsqlSourceResults(esql(query));

        final MatchResult matchResult = Matcher.matchSource()
            .mappings(indexConfig.mapping().lookup(), xcontentMappings, xcontentMappings)
            .settings(actualSettingsBuilder, indexConfig.settings)
            .expected(expectedDocs)
            .ignoringSort(true)
            .isEqualTo(queryHits);
        assertTrue(matchResult.getMessage(), matchResult.isMatch());
    }

    private Response esql(final String query) throws IOException {
        final Request request = new Request("POST", "/_query");
        request.setJsonEntity("{\"query\": \"" + query + "\"}");
        return client().performRequest(request);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> getEsqlSourceResults(final Response response) throws IOException {
        final Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), response.getEntity().getContent(), true);
        final List<List<Object>> values = (List<List<Object>>) map.get("values");
        assertThat(values.size(), greaterThan(0));

        // Results contain a list of [source, id] lists.
        return values.stream()
            .sorted(Comparator.comparing((List<Object> value) -> Integer.valueOf(((String) value.get(1)))))
            .map(value -> (Map<String, Object>) value.get(0))
            .toList();
    }
}
