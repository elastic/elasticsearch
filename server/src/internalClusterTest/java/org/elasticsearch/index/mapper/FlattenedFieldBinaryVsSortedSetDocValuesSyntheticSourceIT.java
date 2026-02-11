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
import org.elasticsearch.datageneration.datasource.ASCIIStringsHandler;
import org.elasticsearch.datageneration.datasource.DataSource;
import org.elasticsearch.datageneration.matchers.MatchResult;
import org.elasticsearch.datageneration.matchers.Matcher;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESSingleNodeTestCase;
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

    private static final String FLATTENED_FIELD_NAME = "data";
    private static final String SORTED_SET_INDEX = "test-index-sorted-set";
    private static final String BINARY_INDEX = "test-index-binary";

    private static final Template TEMPLATE = new Template(
        Map.of(FLATTENED_FIELD_NAME, new Template.Leaf(FLATTENED_FIELD_NAME, "flattened"))
    );

    private XContentBuilder createIndices() throws IOException {
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

        return mapping;
    }

    private List<Map<String, Object>> generateDocs(DataGeneratorSpecification spec, Mapping mapping, int docCount) throws IOException {
        DocumentGenerator documentGenerator = new DocumentGenerator(spec);
        List<Map<String, Object>> docs = new ArrayList<>(docCount);
        for (int i = 0; i < docCount; i++) {
            var document = documentGenerator.generate(TEMPLATE, mapping);
            docs.add(document);
        }
        return docs;
    }

    private void indexDocuments(String index, List<Map<String, Object>> docs) {
        BulkRequestBuilder bulkRequest = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        int id = 0;
        for (var doc : docs) {
            bulkRequest.add(prepareIndex(index).setId(Integer.toString(id++)).setSource(doc));
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

    private void compareDocuments(
        int docCount,
        Mapping mapping,
        XContentBuilder xcontentMapping,
        List<Map<String, Object>> sortedSetDocs,
        List<Map<String, Object>> binaryDocs
    ) {
        assertEquals("Unexpected doc count from sorted set doc values index", docCount, sortedSetDocs.size());
        assertEquals("Unexpected doc count from binary doc values index", docCount, binaryDocs.size());

        var settings = Settings.builder().put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "synthetic");

        final MatchResult matchResult = Matcher.matchSource()
            .mappings(mapping.lookup(), xcontentMapping, xcontentMapping)
            .settings(settings, settings)
            .expected(sortedSetDocs)
            .ignoringSort(true)
            .isEqualTo(binaryDocs);
        assertTrue(matchResult.getMessage(), matchResult.isMatch());
    }

    public void testSyntheticSource() throws IOException {
        DataSource dataSource = new DataSource(List.of(new ASCIIStringsHandler()));
        DataGeneratorSpecification spec = new DataGeneratorSpecification(dataSource, 8, 4, 0, false, Collections.emptyList());
        MappingGenerator mappingGenerator = new MappingGenerator(spec);

        Mapping mapping = mappingGenerator.generate(TEMPLATE);

        var xcontentMappings = createIndices();
        var docs = generateDocs(spec, mapping, 32);

        indexDocuments(SORTED_SET_INDEX, docs);
        indexDocuments(BINARY_INDEX, docs);

        var sortedSetDocs = queryIndex(SORTED_SET_INDEX, docs.size());
        var binaryDocs = queryIndex(BINARY_INDEX, docs.size());

        compareDocuments(docs.size(), mapping, xcontentMappings, sortedSetDocs, binaryDocs);
    }

}
