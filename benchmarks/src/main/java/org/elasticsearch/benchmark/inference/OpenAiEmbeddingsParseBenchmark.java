/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.inference;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Measures the steady-state throughput and per-op allocation of three decoders
 * for an OpenAI-compatible {@code embedding} field, all producing a primitive
 * {@code float[]} as the final output:
 *
 * <ul>
 *   <li>{@code legacy}: the pre-refactor inner field decoder. Walks a JSON
 *       float array with {@link XContentParser#floatValue()}, accumulating
 *       into an {@link ArrayList}{@code <Float>} (the same shape
 *       {@code ConstructingObjectParser.declareFloatArray} produces), then
 *       unboxes element-wise into a {@code float[]}. This is the
 *       {@code List<Float>} -> {@code float[]} conversion that the refactor
 *       eliminates.</li>
 *   <li>{@code new_json}: the current token-driven JSON-array decoder.
 *       Grows a primitive {@code float[]} buffer geometrically from an
 *       initial capacity of {@code 1024}, then final-trims. Same JSON wire
 *       shape as {@code legacy}, but no boxing.</li>
 *   <li>{@code new_base64}: the current base64 decoder. Decodes the
 *       embedding field's string token in one shot via
 *       {@code Base64.getDecoder().decode(...)} then
 *       {@code ByteBuffer.LITTLE_ENDIAN.asFloatBuffer().get(...)}. This is
 *       the wire shape OpenAI and Azure OpenAI return now that the request
 *       carries {@code encoding_format=base64}.</li>
 * </ul>
 *
 * <p>The benchmark deliberately strips away the outer
 * {@code ConstructingObjectParser} envelope that production uses (and that
 * is structurally identical between the pre- and post-refactor parsers) so
 * that the only thing measured is the inner {@code embedding}-field decoder.
 * Each invocation pays the same fixed three-token traversal cost
 * ({@code START_OBJECT}, {@code FIELD_NAME}, embedding value token), which
 * cancels in the delta between impls.
 *
 * <p>The {@code dims} parameter mirrors the production embedding dimensions
 * that exercise distinct buffer-growth profiles in the JSON-array path
 * (1024 fits exactly, 1536 takes one grow, 3072 takes two grows). The
 * base64 path is unaffected by buffer growth and is included at the same
 * dimensions for like-for-like comparison.
 *
 * <p>The {@link #selfTest()} runs before any benchmark iteration and
 * exercises all impl/dims combinations end-to-end, asserting the parsed
 * {@code float[]} matches the synthetic input vector.
 */
@Fork(1)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class OpenAiEmbeddingsParseBenchmark {

    private static final String EMBEDDING_FIELD = "e";
    private static final int INITIAL_BUFFER_SIZE = 1024;

    static {
        Utils.configureBenchmarkLogging();
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            selfTest();
        }
    }

    public enum Impl {
        /** Pre-refactor {@code List<Float>} -> {@code float[]} pipeline. */
        LEGACY,
        /** Current token-driven JSON-array -> {@code float[]} decoder. */
        NEW_JSON,
        /** Current base64 -> {@code float[]} decoder. */
        NEW_BASE64
    }

    @Param({ "1024", "1536", "3072" })
    public int dims;

    @Param({ "LEGACY", "NEW_JSON", "NEW_BASE64" })
    public Impl impl;

    private byte[] body;
    private float[] expected;

    @Setup
    public void setup() {
        this.expected = syntheticVector(dims);
        this.body = switch (impl) {
            case LEGACY, NEW_JSON -> jsonArrayBody(expected);
            case NEW_BASE64 -> base64Body(expected);
        };
    }

    @Benchmark
    public int run() throws IOException {
        float[] out = switch (impl) {
            case LEGACY -> parseLegacy(body);
            case NEW_JSON, NEW_BASE64 -> parseNew(body);
        };
        return out.length;
    }

    /**
     * Pre-refactor inner decoder: walks the JSON array element by element
     * with {@link XContentParser#floatValue()}, autoboxing into an
     * {@link ArrayList}{@code <Float>}, then unboxes into a {@code float[]}.
     * This is literally what
     * {@code ConstructingObjectParser.declareFloatArray} produced followed
     * by the unbox loop in {@code EmbeddingFloatResults.Embedding.of}.
     */
    static float[] parseLegacy(byte[] bytes) throws IOException {
        try (XContentParser p = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, bytes)) {
            advanceToEmbeddingValue(p);
            List<Float> list = new ArrayList<>();
            while (p.nextToken() != XContentParser.Token.END_ARRAY) {
                list.add(p.floatValue());
            }
            float[] out = new float[list.size()];
            for (int i = 0; i < list.size(); i++) {
                out[i] = list.get(i);
            }
            return out;
        }
    }

    /**
     * Current token-driven decoder. Branches on the {@code embedding} field
     * value token: a JSON {@code START_ARRAY} runs {@link #decodeJsonArray},
     * a {@code VALUE_STRING} runs {@link #decodeBase64}. Mirrors production
     * line-for-line.
     */
    static float[] parseNew(byte[] bytes) throws IOException {
        try (XContentParser p = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, bytes)) {
            XContentParser.Token token = advanceToEmbeddingValue(p);
            return switch (token) {
                case VALUE_STRING -> decodeBase64(p);
                case START_ARRAY -> decodeJsonArray(p);
                default -> throw new IllegalStateException("unexpected token: " + token);
            };
        }
    }

    private static float[] decodeBase64(XContentParser p) throws IOException {
        byte[] raw = Base64.getDecoder().decode(p.text());
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

    /**
     * Advance through the minimal {@code {"e": <value>}} envelope and leave
     * the parser positioned at the value token. The envelope is fixed-cost
     * work that is identical across all three impls.
     */
    private static XContentParser.Token advanceToEmbeddingValue(XContentParser p) throws IOException {
        p.nextToken(); // START_OBJECT
        p.nextToken(); // FIELD_NAME ("e")
        return p.nextToken(); // value token (START_ARRAY or VALUE_STRING)
    }

    static byte[] jsonArrayBody(float[] vector) {
        StringBuilder sb = new StringBuilder(16 + vector.length * 16);
        sb.append("{\"").append(EMBEDDING_FIELD).append("\":[");
        for (int i = 0; i < vector.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(vector[i]);
        }
        sb.append("]}");
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    static byte[] base64Body(float[] vector) {
        ByteBuffer bb = ByteBuffer.allocate(Float.BYTES * vector.length).order(ByteOrder.LITTLE_ENDIAN);
        for (float v : vector) {
            bb.putFloat(v);
        }
        String b64 = Base64.getEncoder().encodeToString(bb.array());
        return ("{\"" + EMBEDDING_FIELD + "\":\"" + b64 + "\"}").getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Deterministic synthetic embedding vector. Values are bounded to roughly
     * the [-0.5, 0.5] range that production text embeddings tend to occupy
     * and are chosen to be exactly representable in single precision so the
     * legacy decimal-text path round-trips identically to the base64 byte
     * path.
     */
    static float[] syntheticVector(int dims) {
        float[] vec = new float[dims];
        for (int i = 0; i < dims; i++) {
            vec[i] = ((float) ((i * 2654435761L) & 0xFFFF)) / 0xFFFF - 0.5f;
        }
        return vec;
    }

    static void selfTest() {
        for (String dimsParam : Utils.possibleValues(OpenAiEmbeddingsParseBenchmark.class, "dims")) {
            for (Impl impl : Impl.values()) {
                String label = "[" + impl + "/" + dimsParam + "]";
                OpenAiEmbeddingsParseBenchmark b = new OpenAiEmbeddingsParseBenchmark();
                b.dims = Integer.parseInt(dimsParam);
                b.impl = impl;
                try {
                    b.setup();
                    float[] out = switch (impl) {
                        case LEGACY -> parseLegacy(b.body);
                        case NEW_JSON, NEW_BASE64 -> parseNew(b.body);
                    };
                    if (out.length != b.expected.length) {
                        throw new AssertionError(label + " length " + out.length + " != " + b.expected.length);
                    }
                    for (int i = 0; i < out.length; i++) {
                        if (out[i] != b.expected[i]) {
                            throw new AssertionError(label + " mismatch at " + i + ": got " + out[i] + " expected " + b.expected[i]);
                        }
                    }
                } catch (Exception e) {
                    throw new AssertionError("error running " + label, e);
                }
            }
        }
    }
}
