/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.response;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class OpenAiEmbeddingsResponseEntity {
    /**
     * Parses an OpenAI-compatible embeddings response.
     *
     * <p>The {@code embedding} field on each entry is accepted in either of two shapes:
     *
     * <ul>
     *   <li>a base64-encoded packed little-endian {@code float32} string (what
     *       OpenAI and Azure OpenAI return when the request carries
     *       {@code encoding_format=base64}; this is the OpenAI Python SDK
     *       default), or</li>
     *   <li>a JSON array of floats (the shape every other OpenAI-compatible
     *       provider that shares this parser still returns, e.g. Mistral,
     *       Fireworks, OpenShift AI, Azure AI Studio, SageMaker).</li>
     * </ul>
     *
     * <p>Both shapes decode directly into a primitive {@code float[]} on the
     * resulting {@link EmbeddingFloatResults.Embedding}.
     *
     * <p>Example happy-path response (base64 shape):
     *
     * <pre>
     * <code>
     * {
     *  "object": "list",
     *  "data": [
     *      {
     *          "object": "embedding",
     *          "embedding": "AACAPwAAAEAAAEBA",
     *          "index": 0
     *      }
     *  ],
     *  "model": "text-embedding-3-large",
     *  "usage": { "prompt_tokens": 8, "total_tokens": 8 }
     * }
     * </code>
     * </pre>
     */
    public static EmbeddingFloatResults fromResponse(OutboundRequest outboundRequest, HttpResult response) throws IOException {
        try (var p = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, response.body())) {
            var result = parse(p);
            if (outboundRequest.getTaskType().equals(TaskType.TEXT_EMBEDDING)) {
                return result.toDenseEmbeddingFloatResults();
            } else {
                return result.toGenericDenseEmbeddingFloatResults();
            }
        }
    }

    /**
     * Public entry point for callers that already hold an {@link XContentParser}
     * (e.g. SageMaker's payload adapter). Mirrors {@link #fromResponse} but
     * leaves task-type dispatch to the caller.
     */
    public static EmbeddingFloatResult parse(XContentParser p) throws IOException {
        return EmbeddingFloatResult.PARSER.apply(p, null);
    }

    public record EmbeddingFloatResult(List<EmbeddingFloatResultEntry> embeddingResults) {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<EmbeddingFloatResult, Void> PARSER = new ConstructingObjectParser<>(
            EmbeddingFloatResult.class.getSimpleName(),
            true,
            args -> new EmbeddingFloatResult((List<EmbeddingFloatResultEntry>) args[0])
        );

        static {
            PARSER.declareObjectArray(constructorArg(), (p, c) -> EmbeddingFloatResultEntry.parse(p), new ParseField("data"));
        }

        public DenseEmbeddingFloatResults toDenseEmbeddingFloatResults() {
            return new DenseEmbeddingFloatResults(
                embeddingResults.stream().map(entry -> new EmbeddingFloatResults.Embedding(entry.embedding)).toList()
            );
        }

        public GenericDenseEmbeddingFloatResults toGenericDenseEmbeddingFloatResults() {
            return new GenericDenseEmbeddingFloatResults(
                embeddingResults.stream().map(entry -> new EmbeddingFloatResults.Embedding(entry.embedding)).toList()
            );
        }
    }

    public record EmbeddingFloatResultEntry(float[] embedding) {

        private static final String EMBEDDING_FIELD = "embedding";

        /**
         * Initial size of the working buffer used by the JSON-array branch.
         * Sized to match the modal embedding dimension across providers that
         * share this parser (e.g. Mistral {@code mistral-embed}, Cohere v3,
         * Voyage, Jina v3, mxbai-embed-large at 1024 dims, which fit exactly
         * with no grow and no trim). Larger models pay one or more geometric
         * grows: {@code text-embedding-ada-002} and {@code text-embedding-3-small}
         * at 1536 take one grow to 2048; {@code text-embedding-3-large} at 3072
         * takes two grows to 4096. Smaller models leave slack that is trimmed
         * at the end. The buffer doubles geometrically on overflow and is
         * final-trimmed to the exact dimension before being handed to the caller.
         */
        private static final int INITIAL_BUFFER_SIZE = 1024;

        /**
         * Token-driven parser for one {@code data[i]} entry. Accepts the
         * {@code embedding} field as either a base64 string or a JSON float
         * array; in both shapes it produces a primitive {@code float[]}
         * directly, avoiding any {@code List<Float>} / boxing on the hot
         * path.
         *
         * <p>The enclosing {@link EmbeddingFloatResult#PARSER} wraps any
         * exception thrown here in an {@link XContentParseException} with a
         * {@code [EmbeddingFloatResult] failed to parse field [data]} prefix.
         */
        public static EmbeddingFloatResultEntry parse(XContentParser p) throws IOException {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, p.currentToken(), p);

            float[] embedding = null;
            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = p.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = p.currentName();
                } else if (EMBEDDING_FIELD.equals(currentFieldName)) {
                    embedding = switch (token) {
                        case VALUE_STRING -> decodeBase64(p);
                        case START_ARRAY -> decodeJsonArray(p);
                        default -> throw new XContentParseException(
                            p.getTokenLocation(),
                            "[" + EMBEDDING_FIELD + "] expected base64 string or float array, got token [" + token + "]"
                        );
                    };
                } else {
                    p.skipChildren();
                }
            }

            if (embedding == null) {
                throw new XContentParseException(p.getTokenLocation(), "Required [" + EMBEDDING_FIELD + "]");
            }
            return new EmbeddingFloatResultEntry(embedding);
        }

        private static float[] decodeBase64(XContentParser p) throws IOException {
            byte[] raw;
            try {
                raw = Base64.getDecoder().decode(p.text());
            } catch (IllegalArgumentException e) {
                throw new XContentParseException(p.getTokenLocation(), "[" + EMBEDDING_FIELD + "] is not valid base64: " + e.getMessage());
            }
            if ((raw.length & 0x3) != 0) {
                throw new XContentParseException(
                    p.getTokenLocation(),
                    "[" + EMBEDDING_FIELD + "] base64-decoded length [" + raw.length + "] is not a multiple of 4"
                );
            }
            float[] out = new float[raw.length >>> 2];
            ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(out);
            return out;
        }

        private static float[] decodeJsonArray(XContentParser p) throws IOException {
            float[] buf = new float[INITIAL_BUFFER_SIZE];
            int n = 0;
            for (XContentParser.Token t = p.nextToken(); t != XContentParser.Token.END_ARRAY; t = p.nextToken()) {
                if (n == buf.length) {
                    buf = Arrays.copyOf(buf, buf.length << 1);
                }
                buf[n++] = p.floatValue();
            }
            return (n == buf.length) ? buf : Arrays.copyOf(buf, n);
        }
    }

    private OpenAiEmbeddingsResponseEntity() {}
}
