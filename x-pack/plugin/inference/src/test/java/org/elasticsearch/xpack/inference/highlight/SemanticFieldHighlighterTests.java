/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.highlight;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.mapper.SemanticFieldMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class SemanticFieldHighlighterTests extends AbstractSemanticHighlighterTests {
    private static final String SEMANTIC_FIELD_IMAGE = "field-semantic-image";
    private static final String IMAGE_DATA_URL =
        "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=";

    public SemanticFieldHighlighterTests() throws IOException {
        super(
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build(),
            Streams.readFully(SemanticFieldHighlighterTests.class.getResourceAsStream("mappings-semantic.json")).utf8ToString(),
            sourceFromFile(SemanticFieldHighlighterTests.class.getResourceAsStream("sample-doc-semantic.json.gz")),
            denseVectorQueryData()
        );
    }

    /**
     * Image values are not chunked, so a single input produces a single chunk whose highlight is the entire
     * original data URL rather than a substring of it.
     */
    public void testImage() throws Exception {
        var fieldType = (SemanticFieldMapper.SemanticFieldType) mapperService.mappingLookup().getFieldType(SEMANTIC_FIELD_IMAGE);

        float[] vector = new float[384];
        Arrays.fill(vector, 1.0f);
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

        XContentBuilder sourceBuilder = XContentFactory.jsonBuilder().startObject();
        sourceBuilder.startObject(SEMANTIC_FIELD_IMAGE).field("type", "image").field("value", IMAGE_DATA_URL).endObject();
        sourceBuilder.startObject("_inference_fields");
        {
            sourceBuilder.startObject(SEMANTIC_FIELD_IMAGE);
            {
                sourceBuilder.startObject("inference");
                sourceBuilder.field("inference_id", ".omni-model-id");
                sourceBuilder.startObject("model_settings");
                sourceBuilder.field("task_type", "embedding");
                sourceBuilder.field("dimensions", 384);
                sourceBuilder.field("similarity", "cosine");
                sourceBuilder.field("element_type", "float");
                sourceBuilder.endObject();
                sourceBuilder.startObject("chunks");
                sourceBuilder.startArray(SEMANTIC_FIELD_IMAGE);
                {
                    sourceBuilder.startObject();
                    sourceBuilder.field("input_index", 0);
                    sourceBuilder.array("embeddings", vector);
                    sourceBuilder.endObject();
                }
                sourceBuilder.endArray();
                sourceBuilder.endObject();
                sourceBuilder.endObject();
            }
            sourceBuilder.endObject();
        }
        sourceBuilder.endObject();
        sourceBuilder.endObject();
        var sourceToParse = new SourceToParse("0", BytesReference.bytes(sourceBuilder), XContentType.JSON);

        assertHighlightOneDoc(
            mapperService,
            createSearchExecutionContext(mapperService),
            shardRequest,
            sourceToParse,
            SEMANTIC_FIELD_IMAGE,
            1,
            HighlightBuilder.Order.SCORE,
            new String[] { IMAGE_DATA_URL }
        );
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> denseVectorQueryData() throws IOException {
        var input = Streams.readFully(SemanticFieldHighlighterTests.class.getResourceAsStream("queries.json"));
        var map = XContentHelper.convertToMap(input, false, XContentType.JSON).v2();
        return (Map<String, Object>) map.get("dense_vector_1");
    }
}
