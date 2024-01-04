/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.mapper;

import org.apache.commons.math3.util.Pair;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MetadataMapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.LeafNestedDocuments;
import org.elasticsearch.search.NestedDocuments;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SemanticTextInferenceResultFieldMapperTests extends MetadataMapperTestCase {
    private record SemanticTextInferenceResults(String fieldName, List<Pair<Map<String, Float>, String>> inferenceResults) {}

    @Override
    protected String fieldName() {
        return SemanticTextInferenceResultFieldMapper.NAME;
    }

    @Override
    protected boolean isConfigurable() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {

    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new MachineLearning(Settings.EMPTY));
    }

    public void testSuccessfulParse() throws IOException {
        final String fieldName = "inference_field";
        MapperService mapperService = createMapperService(mapping(b -> addSemanticTextMapping(b, fieldName, "test-model")));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> addSemanticTextInferenceResults(b, List.of(
            new SemanticTextInferenceResults(fieldName, List.of(
                new Pair<>(Map.of(
                    "a", 0.5f,
                    "b", 0.9f
                ),
                    "a b"
                )
            ))
        ))));

        List<LuceneDocument> luceneDocs = doc.docs();
        assertEquals(2, luceneDocs.size());
        assertEquals(2, luceneDocs.get(0).getFields(
            fieldName + "." + SemanticTextInferenceResultFieldMapper.SPARSE_VECTOR_SUBFIELD_NAME).size());
        assertEquals(fieldName, luceneDocs.get(0).getPath());
        assertEquals(doc.rootDoc(), luceneDocs.get(0).getParent());
        assertEquals(doc.rootDoc(), luceneDocs.get(1));
        assertNull(luceneDocs.get(1).getParent());

        MapperService nestedMapperService = createMapperService(mapping(b -> {
            addInferenceResultsNestedMapping(b, fieldName);
        }));
        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), reader -> {
            NestedDocuments nested = new NestedDocuments(
                nestedMapperService.mappingLookup(),
                QueryBitSetProducer::new,
                IndexVersion.current());
            LeafNestedDocuments leaf = nested.getLeafNestedDocuments(reader.leaves().get(0));

            assertNotNull(leaf.advance(0));
            assertEquals(0, leaf.doc());
            assertEquals(1, leaf.rootDoc());
            assertEquals(new SearchHit.NestedIdentity(fieldName, 0, null), leaf.nestedIdentity());

            assertNull(leaf.advance(1));
            assertEquals(1, leaf.doc());
            assertEquals(1, leaf.doc());
            assertNull(leaf.nestedIdentity());
        });
    }

    private static void addSemanticTextMapping(XContentBuilder mappingBuilder, String fieldName, String modelId) throws IOException {
        mappingBuilder.startObject(fieldName);
        mappingBuilder.field("type", SemanticTextFieldMapper.CONTENT_TYPE);
        mappingBuilder.field("model_id", modelId);
        mappingBuilder.endObject();
    }

    private static void addSemanticTextInferenceResults(
        XContentBuilder sourceBuilder,
        List<SemanticTextInferenceResults> semanticTextInferenceResults) throws IOException {

        Map<String, List<Map<String, Object>>> inferenceResultsMap = new HashMap<>();
        for (SemanticTextInferenceResults semanticTextInferenceResult : semanticTextInferenceResults) {
            List<Map<String, Object>> parsedInferenceResults = new ArrayList<>(semanticTextInferenceResult.inferenceResults().size());
            for (Pair<Map<String, Float>, String> inferenceResults : semanticTextInferenceResult.inferenceResults()) {
                parsedInferenceResults.add(Map.of(
                    SemanticTextInferenceResultFieldMapper.SPARSE_VECTOR_SUBFIELD_NAME, inferenceResults.getFirst(),
                    SemanticTextInferenceResultFieldMapper.TEXT_SUBFIELD_NAME, inferenceResults.getSecond()
                ));
            }

            inferenceResultsMap.put(semanticTextInferenceResult.fieldName(), parsedInferenceResults);
        }

        sourceBuilder.field(SemanticTextInferenceResultFieldMapper.NAME, inferenceResultsMap);
    }

    private static void addInferenceResultsNestedMapping(XContentBuilder mappingBuilder, String semanticTextFieldName) throws IOException {
        mappingBuilder.startObject(semanticTextFieldName);
        mappingBuilder.field("type", "nested");
        mappingBuilder.startObject("properties");
        mappingBuilder.startObject(SemanticTextInferenceResultFieldMapper.SPARSE_VECTOR_SUBFIELD_NAME);
        mappingBuilder.field("type", "sparse_vector");
        mappingBuilder.endObject();
        mappingBuilder.startObject(SemanticTextInferenceResultFieldMapper.TEXT_SUBFIELD_NAME);
        mappingBuilder.field("type", "text");
        mappingBuilder.field("index", false);
        mappingBuilder.endObject();
        mappingBuilder.endObject();
        mappingBuilder.endObject();
    }
}
