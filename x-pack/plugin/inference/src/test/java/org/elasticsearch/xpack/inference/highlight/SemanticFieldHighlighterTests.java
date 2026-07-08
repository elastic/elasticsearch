/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.highlight;

import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.mapper.SemanticFieldMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class SemanticFieldHighlighterTests extends AbstractSemanticHighlighterTests {

    public SemanticFieldHighlighterTests() throws IOException {
    }

    @Override
    MapperService createMapperService() throws IOException {
        var mappings = Streams.readFully(SemanticFieldHighlighterTests.class.getResourceAsStream("mappings-semantic.json"));
        return createMapperService(Settings.EMPTY, mappings.utf8ToString());
    }

    @Override
    BytesReference readSampleDoc() throws IOException {
        try (var in = new GZIPInputStream(SemanticFieldHighlighterTests.class.getResourceAsStream("sample-doc-semantic.json.gz"))) {
            return new BytesArray(new BytesRef(in.readAllBytes()));
        }
    }

    @SuppressWarnings("unchecked")
    public void testDenseVectorWithSimilarityThreshold() throws Exception {
        var mapperService = createMapperService();
        Map<String, Object> queryMap = (Map<String, Object>) queries.get("dense_vector_1");
        float[] vector = readDenseVector(queryMap.get("embeddings"));
        var fieldType = (SemanticFieldMapper.SemanticFieldType) mapperService.mappingLookup().getFieldType(SEMANTIC_FIELD_E5);

        KnnVectorQueryBuilder knnQuery = new KnnVectorQueryBuilder(
            fieldType.getEmbeddingsField().fullPath(),
            vector,
            10,
            10,
            10f,
            null,
            0.85f
        );
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(fieldType.getChunksField().fullPath(), knnQuery, ScoreMode.Max);
        var shardRequest = createShardSearchRequest(nestedQueryBuilder);
        var sourceToParse = new SourceToParse("0", readSampleDoc(), XContentType.JSON);

        String[] expectedPassages = ((List<String>) queryMap.get("expected_with_similarity_threshold")).toArray(String[]::new);
        assertHighlightOneDoc(
            mapperService,
            shardRequest,
            sourceToParse,
            SEMANTIC_FIELD_E5,
            expectedPassages.length,
            HighlightBuilder.Order.SCORE,
            expectedPassages
        );
    }

    @SuppressWarnings("unchecked")
    public void testDenseVectorWithDiskBBQandSimilarityThreshold() throws Exception {
        var mapperService = createMapperService();
        Map<String, Object> queryMap = (Map<String, Object>) queries.get("dense_vector_1");
        float[] vector = readDenseVector(queryMap.get("embeddings"));
        var fieldType = (SemanticFieldMapper.SemanticFieldType) mapperService.mappingLookup()
            .getFieldType(SEMANTIC_FIELD_E5_DISK_BBQ);

        KnnVectorQueryBuilder knnQuery = new KnnVectorQueryBuilder(
            fieldType.getEmbeddingsField().fullPath(),
            vector,
            10,
            10,
            10f,
            null,
            0.85f
        );
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(fieldType.getChunksField().fullPath(), knnQuery, ScoreMode.Max);
        var shardRequest = createShardSearchRequest(nestedQueryBuilder);
        var sourceToParse = new SourceToParse("0", readSampleDoc(), XContentType.JSON);

        String[] expectedPassages = ((List<String>) queryMap.get("expected_with_similarity_threshold")).toArray(String[]::new);
        assertHighlightOneDoc(
            mapperService,
            shardRequest,
            sourceToParse,
            SEMANTIC_FIELD_E5_DISK_BBQ,
            expectedPassages.length,
            HighlightBuilder.Order.SCORE,
            expectedPassages
        );
    }

    @SuppressWarnings("unchecked")
    public void testDenseVectorWithDiskBBQ() throws Exception {
        var mapperService = createMapperService();
        Map<String, Object> queryMap = (Map<String, Object>) queries.get("dense_vector_1");
        float[] vector = readDenseVector(queryMap.get("embeddings"));
        var fieldType = (SemanticFieldMapper.SemanticFieldType) mapperService.mappingLookup()
            .getFieldType(SEMANTIC_FIELD_E5_DISK_BBQ);

        KnnVectorQueryBuilder knnQuery = new KnnVectorQueryBuilder(
            fieldType.getEmbeddingsField().fullPath(),
            vector,
            10,
            10,
            10f,
            null,
            null
        );
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(fieldType.getChunksField().fullPath(), knnQuery, ScoreMode.Max);
        var shardRequest = createShardSearchRequest(nestedQueryBuilder);
        var sourceToParse = new SourceToParse("0", readSampleDoc(), XContentType.JSON);

        String[] expectedScorePassages = ((List<String>) queryMap.get("expected_by_score")).toArray(String[]::new);
        for (int i = 0; i < expectedScorePassages.length; i++) {
            assertHighlightOneDoc(
                mapperService,
                shardRequest,
                sourceToParse,
                SEMANTIC_FIELD_E5_DISK_BBQ,
                i + 1,
                HighlightBuilder.Order.SCORE,
                Arrays.copyOfRange(expectedScorePassages, 0, i + 1)
            );
        }

        String[] expectedOffsetPassages = ((List<String>) queryMap.get("expected_by_offset")).toArray(String[]::new);
        assertHighlightOneDoc(
            mapperService,
            shardRequest,
            sourceToParse,
            SEMANTIC_FIELD_E5_DISK_BBQ,
            expectedOffsetPassages.length,
            HighlightBuilder.Order.NONE,
            expectedOffsetPassages
        );
    }

}
