/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.response;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.response.XContentUtils;
import org.elasticsearch.xpack.inference.services.tencentcloud.request.TencentCloudMultimodalEmbeddingsRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseList;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

/**
 * Parses a TencentCloud {@code /v1/multimodal-embeddings} response in separate mode.
 * <pre>
 * {
 *   "object": "multimodal_embedding",
 *   "data": {
 *     "text_embeddings":  [ { "object": "embedding", "embedding": [0.0023, -0.0099], "index": 0 } ],
 *     "image_embeddings": [ { "object": "embedding", "embedding": [0.0015, 0.0032], "index": 0 } ],
 *     "video_embeddings": [ { "object": "embedding", "embedding": [0.0036, 0.0346], "index": 0 } ]
 *   },
 *   "model": "WeMM-Embedding-2B",
 *   "usage": { "total_tokens": 7887 }
 * }
 * </pre>
 * Each list is numbered from 0 within its modality. The embeddings are returned in the order of the request inputs.
 */
public class TencentCloudMultimodalEmbeddingsResponseEntity {

    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in TencentCloud embeddings response";

    private static final Map<String, DataType> EMBEDDINGS_FIELDS = Map.of(
        "text_embeddings",
        DataType.TEXT,
        "image_embeddings",
        DataType.IMAGE,
        "video_embeddings",
        DataType.VIDEO
    );

    public static GenericDenseEmbeddingFloatResults fromResponse(OutboundRequest request, HttpResult response) throws IOException {
        var inputs = ((TencentCloudMultimodalEmbeddingsRequest) request).getInputs();
        var embeddingsByType = parseEmbeddingsByType(response);

        Map<DataType, Integer> nextIndex = new EnumMap<>(DataType.class);
        List<EmbeddingFloatResults.Embedding> embeddings = new ArrayList<>(inputs.size());
        for (InferenceString input : inputs) {
            var type = input.dataType();
            int index = nextIndex.merge(type, 1, Integer::sum) - 1;
            var typeEmbeddings = embeddingsByType.getOrDefault(type, List.of());
            if (index >= typeEmbeddings.size()) {
                throw new IllegalStateException(
                    Strings.format("TencentCloud embeddings response is missing the [%s] embedding at index [%d]", type, index)
                );
            }
            embeddings.add(typeEmbeddings.get(index));
        }
        for (var entry : embeddingsByType.entrySet()) {
            int expected = nextIndex.getOrDefault(entry.getKey(), 0);
            if (entry.getValue().size() != expected) {
                throw new IllegalStateException(
                    Strings.format(
                        "TencentCloud embeddings response contains [%d] [%s] embeddings but [%d] were requested",
                        entry.getValue().size(),
                        entry.getKey(),
                        expected
                    )
                );
            }
        }
        return new GenericDenseEmbeddingFloatResults(embeddings);
    }

    private static Map<DataType, List<EmbeddingFloatResults.Embedding>> parseEmbeddingsByType(HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(parser);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
            positionParserAtTokenAfterField(parser, "data", FAILED_TO_FIND_FIELD_TEMPLATE);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

            Map<DataType, List<EmbeddingFloatResults.Embedding>> embeddingsByType = new EnumMap<>(DataType.class);
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
                var type = EMBEDDINGS_FIELDS.get(parser.currentName());
                parser.nextToken();
                if (type == null) {
                    parser.skipChildren();
                    continue;
                }
                embeddingsByType.put(type, placeByIndex(parseList(parser, TencentCloudMultimodalEmbeddingsResponseEntity::parseEmbedding)));
            }
            return embeddingsByType;
        }
    }

    private static List<EmbeddingFloatResults.Embedding> placeByIndex(List<IndexedEmbedding> indexedEmbeddings) {
        List<EmbeddingFloatResults.Embedding> embeddings = new ArrayList<>(indexedEmbeddings.size());
        for (int i = 0; i < indexedEmbeddings.size(); i++) {
            embeddings.add(null);
        }
        for (var indexedEmbedding : indexedEmbeddings) {
            int index = indexedEmbedding.index();
            if (index < 0 || index >= embeddings.size() || embeddings.get(index) != null) {
                throw new IllegalStateException(Strings.format("TencentCloud embeddings response contains an invalid index [%d]", index));
            }
            embeddings.set(index, indexedEmbedding.embedding());
        }
        return embeddings;
    }

    private static IndexedEmbedding parseEmbedding(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        Integer index = null;
        List<Float> values = null;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
            var field = parser.currentName();
            parser.nextToken();
            switch (field) {
                case "index" -> index = parser.intValue();
                case "embedding" -> values = parseList(parser, XContentUtils::parseFloat);
                default -> parser.skipChildren();
            }
        }
        if (index == null) {
            throw new IllegalStateException(Strings.format(FAILED_TO_FIND_FIELD_TEMPLATE, "index"));
        }
        if (values == null) {
            throw new IllegalStateException(Strings.format(FAILED_TO_FIND_FIELD_TEMPLATE, "embedding"));
        }
        return new IndexedEmbedding(index, EmbeddingFloatResults.Embedding.of(values));
    }

    private record IndexedEmbedding(int index, EmbeddingFloatResults.Embedding embedding) {}

    private TencentCloudMultimodalEmbeddingsResponseEntity() {}
}
