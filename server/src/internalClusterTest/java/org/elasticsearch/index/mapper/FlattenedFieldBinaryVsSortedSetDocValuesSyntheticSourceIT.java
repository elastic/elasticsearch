/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

public class FlattenedFieldBinaryVsSortedSetDocValuesSyntheticSourceIT extends ESSingleNodeTestCase {
    private static final DataSourceHandler FLATTENED_DATA_GENERATOR = new DataSourceHandler() {
        @Override
        public DataSourceResponse.ChildFieldGenerator handle(DataSourceRequest.ChildFieldGenerator request) {
            return new DefaultObjectGenerationHandler.DefaultChildFieldGenerator(request) {
                @Override
                public int generateChildFieldCount() {
                    return ESSingleNodeTestCase.randomIntBetween(0, request.specification().maxFieldCountPerLevel());
                }

                @Override
                public boolean generateDynamicSubObject() {
                    return false;
                }

                @Override
                public boolean generateNestedSubObject() {
                    return false;
                }

                @Override
                public boolean generateRegularSubObject() {
                    return randomDouble() < 0.20; // 20% chance
                }

                @Override
                public String generateFieldName() {
                    while (true) {
                        String candidateName = super.generateFieldName();
                        if (candidateName.contains("\0")) {
                            continue;
                        }

                        return candidateName;
                    }
                }
            };
        }

        @Override
        public DataSourceResponse.FieldTypeGenerator handle(DataSourceRequest.FieldTypeGenerator request) {
            return new DataSourceResponse.FieldTypeGenerator(() -> {
                var fieldType = ESTestCase.randomFrom(List.of("keyword", "text", "long", "double"));
                return new DataSourceResponse.FieldTypeGenerator.FieldTypeInfo(fieldType);
            });
        }
    };

    private static final String FLATTENED_FIELD_NAME = "data";
    private static final String SORTED_SET_INDEX = "test-index-sorted-set";
    private static final String BINARY_INDEX = "test-index-binary";

    private void createIndices() throws IOException {
        CreateIndexResponse createSortedSetIndexResponse = null;
        CreateIndexResponse createBinaryIndexResponse = null;

        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject(FLATTENED_FIELD_NAME)
            .field("type", "flattened")
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        try {
            createSortedSetIndexResponse = indicesAdmin().prepareCreate(SORTED_SET_INDEX)
                .setMapping(mapping)
                .setSettings(
                    Settings.builder()
                        .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "synthetic")
                        .put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING.getKey(), false)
                )
                .get();
            assertAcked(createSortedSetIndexResponse);

            createBinaryIndexResponse = indicesAdmin().prepareCreate(BINARY_INDEX)
                .setMapping(mapping)
                .setSettings(
                    Settings.builder()
                        .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "synthetic")
                        .put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING.getKey(), true)
                )
                .get();
            assertAcked(createBinaryIndexResponse);
        } finally {
            if (createSortedSetIndexResponse != null) {
                createSortedSetIndexResponse.decRef();
            }
            if (createBinaryIndexResponse != null) {
                createBinaryIndexResponse.decRef();
            }
        }
    }

    private record FlattenedData(Template template, Mapping mapping, Map<String, Object> document) {}

    private List<FlattenedData> generateFlattenedData(DataGeneratorSpecification spec, int docCount) throws IOException {
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

    private void indexDocuments(String index, List<FlattenedData> flattenedData) {
        BulkRequestBuilder bulkRequest = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        int id = 0;
        for (var data : flattenedData) {
            bulkRequest.add(prepareIndex(index).setId(Integer.toString(id++)).setSource(Map.of(FLATTENED_FIELD_NAME, data.document())));
        }
        var response = bulkRequest.get();
        assertNoFailures(response);
    }

    private List<Map<String, Object>> queryIndex(String index, int numDocs) {
        SearchResponse response = null;
        try {
            response = client().prepareSearch(index).setFetchSource(true).setSize(numDocs).setQuery(QueryBuilders.matchAllQuery()).get();
            return Arrays.stream(response.getHits().getHits())
                .sorted(Comparator.comparingInt(hit -> Integer.parseInt(hit.getId())))
                .map(SearchHit::getSourceAsMap)
                .toList();
        } finally {
            if (response != null) {
                response.decRef();
            }
        }

    }

    private void compareDocuments(List<FlattenedData> data, List<Map<String, Object>> sortedSetDocs, List<Map<String, Object>> binaryDocs)
        throws IOException {
        assertEquals("Unexpected doc count from sorted set doc values index", data.size(), sortedSetDocs.size());
        assertEquals("Unexpected doc count from binary doc values index", data.size(), binaryDocs.size());

        var settings = Settings.builder().put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "synthetic");

        for (int i = 0; i < data.size(); i++) {
            FlattenedData flattenedData = data.get(i);
            var xcontentMappings = XContentFactory.jsonBuilder().map(flattenedData.mapping().raw());

            @SuppressWarnings("unchecked")
            var expectedDoc = (Map<String, Object>) sortedSetDocs.get(i).get(FLATTENED_FIELD_NAME);
            @SuppressWarnings("unchecked")
            var actualDoc = (Map<String, Object>) binaryDocs.get(i).get(FLATTENED_FIELD_NAME);

            if (expectedDoc == null) {
                assertNull(actualDoc);
                continue;
            }

            final MatchResult matchResult = Matcher.matchSource()
                .mappings(flattenedData.mapping().lookup(), xcontentMappings, xcontentMappings)
                .settings(settings, settings)
                .expected(List.of(expectedDoc))
                .ignoringSort(true)
                .isEqualTo(List.of(actualDoc));
            assertTrue(matchResult.getMessage(), matchResult.isMatch());
        }
    }

    public void testSyntheticSource() throws IOException {
        DataSource dataSource = new DataSource(List.of(FLATTENED_DATA_GENERATOR));
        DataGeneratorSpecification spec = new DataGeneratorSpecification(dataSource, 8, 4, 0, false, Collections.emptyList());

        createIndices();
        var data = generateFlattenedData(spec, 32);

        indexDocuments(SORTED_SET_INDEX, data);
        indexDocuments(BINARY_INDEX, data);

        var sortedSetDocs = queryIndex(SORTED_SET_INDEX, data.size());
        var binaryDocs = queryIndex(BINARY_INDEX, data.size());

        compareDocuments(data, sortedSetDocs, binaryDocs);
    }

}
