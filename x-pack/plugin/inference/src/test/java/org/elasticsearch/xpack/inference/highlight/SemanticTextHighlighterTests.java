/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.highlight;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryBuilder;
import org.elasticsearch.xpack.inference.mapper.SemanticInferenceMetadataFieldsMapperTests;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class SemanticTextHighlighterTests extends AbstractSemanticHighlighterTests {
    private static final String SEMANTIC_FIELD_ELSER = "body-elser";

    private final boolean useLegacyFormat;

    public SemanticTextHighlighterTests(boolean useLegacyFormat) throws IOException {
        this.useLegacyFormat = useLegacyFormat;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return List.of(new Object[] { true }, new Object[] { false });
    }

    @Override
    MapperService createMapperService() throws IOException {
        var mappings = Streams.readFully(AbstractSemanticHighlighterTests.class.getResourceAsStream("mappings.json"));
        if (this.useLegacyFormat) {
            Settings settings = SemanticInferenceMetadataFieldsMapperTests.randomIndexSettings(true);
            MapperService mapperService = createMapperService(
                IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(settings),
                settings,
                mapping(b -> {})
            );
            merge(mapperService, mappings.utf8ToString());
            return mapperService;
        }
        return createMapperService(Settings.EMPTY, mappings.utf8ToString());
    }

    @Override
    BytesReference readSampleDoc() throws IOException {
        var fileName = useLegacyFormat ? "sample-doc-legacy.json.gz" : "sample-doc.json.gz";
        try (var in = new GZIPInputStream(AbstractSemanticHighlighterTests.class.getResourceAsStream(fileName))) {
            return new BytesArray(new BytesRef(in.readAllBytes()));
        }
    }

    @SuppressWarnings("unchecked")
    public void testSparseVector() throws Exception {
        var mapperService = createMapperService();
        Map<String, Object> queryMap = (Map<String, Object>) queries.get("sparse_vector_1");
        List<WeightedToken> tokens = readSparseVector(queryMap.get("embeddings"));
        var fieldType = (SemanticTextFieldMapper.SemanticTextFieldType) mapperService.mappingLookup().getFieldType(SEMANTIC_FIELD_ELSER);
        SparseVectorQueryBuilder sparseQuery = new SparseVectorQueryBuilder(
            fieldType.getEmbeddingsField().fullPath(),
            tokens,
            null,
            null,
            false,
            null
        );
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(fieldType.getChunksField().fullPath(), sparseQuery, ScoreMode.Max);
        var shardRequest = createShardSearchRequest(nestedQueryBuilder);
        var sourceToParse = new SourceToParse("0", readSampleDoc(), XContentType.JSON);

        String[] expectedScorePassages = ((List<String>) queryMap.get("expected_by_score")).toArray(String[]::new);
        for (int i = 0; i < expectedScorePassages.length; i++) {
            assertHighlightOneDoc(
                mapperService,
                shardRequest,
                sourceToParse,
                SEMANTIC_FIELD_ELSER,
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
            SEMANTIC_FIELD_ELSER,
            expectedOffsetPassages.length,
            HighlightBuilder.Order.NONE,
            expectedOffsetPassages
        );
    }

    @SuppressWarnings("unchecked")
    public void testNoSemanticField() throws Exception {
        var mapperService = createMapperService();
        Map<String, Object> queryMap = (Map<String, Object>) queries.get("sparse_vector_1");
        List<WeightedToken> tokens = readSparseVector(queryMap.get("embeddings"));
        var fieldType = (SemanticTextFieldMapper.SemanticTextFieldType) mapperService.mappingLookup().getFieldType(SEMANTIC_FIELD_ELSER);
        SparseVectorQueryBuilder sparseQuery = new SparseVectorQueryBuilder(
            fieldType.getEmbeddingsField().fullPath(),
            tokens,
            null,
            null,
            false,
            null
        );
        var query = new BoolQueryBuilder().should(sparseQuery).should(new MatchAllQueryBuilder());
        var shardRequest = createShardSearchRequest(query);
        var sourceToParse = new SourceToParse("0", new BytesArray("{}"), XContentType.JSON);
        assertHighlightOneDoc(
            mapperService,
            shardRequest,
            sourceToParse,
            SEMANTIC_FIELD_ELSER,
            10,
            HighlightBuilder.Order.SCORE,
            new String[0]
        );
    }

    private List<WeightedToken> readSparseVector(Object value) {
        if (value instanceof Map<?, ?> map) {
            List<WeightedToken> res = new ArrayList<>();
            for (var entry : map.entrySet()) {
                if (entry.getValue() instanceof Number number) {
                    res.add(new WeightedToken((String) entry.getKey(), number.floatValue()));
                } else {
                    throw new IllegalArgumentException("Expected number, got " + entry.getValue().getClass().getSimpleName());
                }
            }
            return res;
        }
        throw new IllegalArgumentException("Expected map, got " + value.getClass().getSimpleName());
    }

}
