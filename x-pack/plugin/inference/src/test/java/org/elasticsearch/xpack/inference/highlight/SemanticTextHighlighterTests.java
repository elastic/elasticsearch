/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.highlight;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryBuilder;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.mapper.SemanticInferenceMetadataFieldsMapperTests.getRandomCompatibleIndexVersion;

public class SemanticTextHighlighterTests extends AbstractSemanticHighlighterTests {
    private static final String SEMANTIC_FIELD_ELSER = "field-sparse-vector";

    private final Map<String, Object> queries;

    public SemanticTextHighlighterTests(boolean useLegacyFormat) throws IOException {
        super(
            indexSettings(useLegacyFormat),
            Streams.readFully(SemanticTextHighlighterTests.class.getResourceAsStream("mappings-semantic_text.json")).utf8ToString(),
            sourceFromFile(
                SemanticTextHighlighterTests.class.getResourceAsStream(
                    useLegacyFormat ? "sample-doc-semantic_text-legacy.json.gz" : "sample-doc-semantic_text.json.gz"
                )
            ),
            denseVectorQueryData()
        );
        this.queries = queryData();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return List.of(new Object[] { true }, new Object[] { false });
    }

    @SuppressWarnings("unchecked")
    public void testSparseVector() throws Exception {
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

        String[] expectedScorePassages = ((List<String>) queryMap.get("expected_by_score")).toArray(String[]::new);
        for (int i = 0; i < expectedScorePassages.length; i++) {
            assertHighlightOneDoc(
                mapperService,
                createSearchExecutionContext(mapperService),
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
            createSearchExecutionContext(mapperService),
            shardRequest,
            sourceToParse,
            SEMANTIC_FIELD_ELSER,
            expectedOffsetPassages.length,
            HighlightBuilder.Order.NONE,
            expectedOffsetPassages
        );
    }

    private static Map<String, Object> queryData() throws IOException {
        var input = Streams.readFully(SemanticTextHighlighterTests.class.getResourceAsStream("queries.json"));
        return XContentHelper.convertToMap(input, false, XContentType.JSON).v2();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> denseVectorQueryData() throws IOException {
        return (Map<String, Object>) queryData().get("dense_vector_1");
    }

    private static Settings indexSettings(boolean useLegacyFormat) {
        var indexVersion = useLegacyFormat ? getRandomCompatibleIndexVersion(true) : IndexVersion.current();
        return Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, indexVersion)
            .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), useLegacyFormat)
            .build();
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
