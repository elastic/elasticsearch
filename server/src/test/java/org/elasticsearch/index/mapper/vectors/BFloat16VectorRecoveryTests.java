/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.codecs.Codec;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.codec.CodecProvider;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.TranslogOperationAsserter;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

/**
 * Tests that {@code TranslogOperationAsserter} accepts the two forms of an operation that
 * mixed-version peer recovery produces for bfloat16 dense_vectors.
 *
 * <p>The same operation can reach a new node twice: through replication with the original source,
 * and through recovery from an older node that synthesized only the indexed vector field. bfloat16
 * values are rounded when they are synthesized, and the indexed field is also normalized for cosine
 * similarity, so synthesizing it again drifts further. As a result no single pairing of the original
 * and synthesized sources matches for the whole document; the indexed field only matches as
 * synthesized(replicated) vs recovered, and the non-indexed field only as replicated vs recovered or
 * synthesized vs synthesized. {@code TranslogOperationAsserter} has to accept this per field.
 *
 * <p>{@code EngineTestCase} picks a random codec, and some (e.g. Lucene's {@code Asserting} codec) store
 * bfloat16 vectors without rounding, which hides the problem. These tests therefore synthesize with
 * the production {@link CodecService#DEFAULT_CODEC}.
 */
public class BFloat16VectorRecoveryTests extends EngineTestCase {

    /** Source as indexed by the client, from {@code search.vectors/200_dense_vector_docvalue_fields}. */
    private static final String ORIGINAL_SOURCE = """
        {"name":"cow.jpg","vector7":[230.0,300.33,-34.8988,15.555,-200.0],"vector8":[130.0,115.0,-1.02,15.555,-100.0]}""";

    /** The same document as a 9.5 node sends it during recovery: only the indexed vector7 is synthesized. */
    private static final String RECOVERED_SOURCE = """
        {"vector8":[130.0,115.0,-1.02,15.555,-100.0],"name":"cow.jpg",\
        "vector7":[229.90425,300.38586,-34.821266,15.522733,-199.69786]}""";

    /** {@link #ORIGINAL_SOURCE} with both vectors moved into an object field. */
    private static final String OBJECT_ORIGINAL_SOURCE = """
        {"name":"cow.jpg","obj":{"vector7":[230.0,300.33,-34.8988,15.555,-200.0],\
        "vector8":[130.0,115.0,-1.02,15.555,-100.0]}}""";

    /** {@link #OBJECT_ORIGINAL_SOURCE} as a 9.5 node sends it during recovery: only the indexed obj.vector7 is synthesized. */
    private static final String OBJECT_RECOVERED_SOURCE = """
        {"obj":{"vector8":[130.0,115.0,-1.02,15.555,-100.0],\
        "vector7":[229.90425,300.38586,-34.821266,15.522733,-199.69786]},"name":"cow.jpg"}""";

    /** Vectors indexed as base64 strings, from {@code search.vectors/205_dense_vector_docvalue_fields_format}. */
    private static final String BASE64_ORIGINAL_SOURCE = """
        {"vec_bfloat16_indexed":"P8BAAMBQ","vec_bfloat16_dv":"P8BAAMBQ"}""";

    /** {@link #BASE64_ORIGINAL_SOURCE} as a 9.4 node sends it during recovery: the indexed vector is synthesized as an array. */
    private static final String BASE64_RECOVERED_SOURCE = """
        {"vec_bfloat16_dv":"P8BAAMBQ","vec_bfloat16_indexed":[1.4975724,2.0021021,-3.2514138]}""";

    @Override
    protected String defaultMapping() {
        return """
            {
              "properties": {
                "name": {
                  "type": "keyword"
                },
                "vector7": {
                  "type": "dense_vector",
                  "element_type": "bfloat16",
                  "dims": 5,
                  "index": true
                },
                "vector8": {
                  "type": "dense_vector",
                  "element_type": "bfloat16",
                  "dims": 5,
                  "index": false
                },
                "vec_bfloat16_indexed": {
                  "type": "dense_vector",
                  "element_type": "bfloat16",
                  "dims": 3,
                  "index": true
                },
                "vec_bfloat16_dv": {
                  "type": "dense_vector",
                  "element_type": "bfloat16",
                  "dims": 3,
                  "index": false
                },
                "obj": {
                  "properties": {
                    "vector7": {
                      "type": "dense_vector",
                      "element_type": "bfloat16",
                      "dims": 5,
                      "index": true
                    },
                    "vector8": {
                      "type": "dense_vector",
                      "element_type": "bfloat16",
                      "dims": 5,
                      "index": false
                    }
                  }
                }
              }
            }
            """;
    }

    public void testTranslogOperationAsserterAcceptsRecoveredSourceFromOlderNode() throws IOException {
        assertSameOperation(ORIGINAL_SOURCE, RECOVERED_SOURCE);
    }

    /**
     * With the vectors inside an object, the object differs in every pairing of whole forms, so the
     * asserter only accepts it by comparing the fields of the object separately.
     */
    public void testTranslogOperationAsserterAcceptsRecoveredSourceWithVectorsInObject() throws IOException {
        assertSameOperation(OBJECT_ORIGINAL_SOURCE, OBJECT_RECOVERED_SOURCE);
    }

    public void testTranslogOperationAsserterAcceptsRecoveredSourceOfBase64Vectors() throws IOException {
        assertSameOperation(BASE64_ORIGINAL_SOURCE, BASE64_RECOVERED_SOURCE);
    }

    public void testTranslogOperationAsserterRejectsDifferentBase64Vector() throws IOException {
        // "P8BAAMBQ" is [1.5, 2.0, -3.25]
        assertDifferentOperation(BASE64_ORIGINAL_SOURCE, """
            {"vec_bfloat16_dv":"P8BAAMBQ","vec_bfloat16_indexed":[2.5,2.0,-3.25]}""");
    }

    public void testTranslogOperationAsserterRejectsDifferentVector() throws IOException {
        assertDifferentOperation(ORIGINAL_SOURCE, """
            {"vector8":[130.0,115.0,-1.02,15.555,-100.0],"name":"cow.jpg",\
            "vector7":[231.0,300.33,-34.8988,15.555,-200.0]}""");
    }

    public void testTranslogOperationAsserterRejectsDifferentVectorInObject() throws IOException {
        assertDifferentOperation(OBJECT_ORIGINAL_SOURCE, """
            {"obj":{"vector8":[130.0,115.0,-1.02,15.555,-100.0],\
            "vector7":[231.0,300.33,-34.8988,15.555,-200.0]},"name":"cow.jpg"}""");
    }

    /**
     * The indexed vector7 is in its recovered form and matches, which must not excuse a different vector8.
     */
    public void testTranslogOperationAsserterRejectsDifferentNonIndexedVector() throws IOException {
        // 130.0 and 131.0 are both exact in bfloat16, so no rounding can make them equal
        assertDifferentOperation(ORIGINAL_SOURCE, """
            {"vector8":[131.0,115.0,-1.02,15.555,-100.0],"name":"cow.jpg",\
            "vector7":[229.90425,300.38586,-34.821266,15.522733,-199.69786]}""");
    }

    /**
     * The same inside an object: a matching obj.vector7 must not excuse a different obj.vector8.
     */
    public void testTranslogOperationAsserterRejectsDifferentNonIndexedVectorInObject() throws IOException {
        assertDifferentOperation(OBJECT_ORIGINAL_SOURCE, """
            {"obj":{"vector8":[131.0,115.0,-1.02,15.555,-100.0],\
            "vector7":[229.90425,300.38586,-34.821266,15.522733,-199.69786]},"name":"cow.jpg"}""");
    }

    /**
     * Asserts that the asserter accepts the two sources as the same operation, whichever of the two
     * copies reached the node first.
     */
    private void assertSameOperation(String replicatedSource, String recoveredSource) throws IOException {
        Engine.IndexResult result = index(replicatedSource);
        Translog.Index replicatedOp = indexOp(result, replicatedSource);
        Translog.Index recoveredOp = indexOp(result, recoveredSource);
        TranslogOperationAsserter asserter = TranslogOperationAsserter.withEngineConfig(productionCodecConfig());

        String message = "replicated source: " + replicatedSource + " recovered source: " + recoveredSource;
        assertTrue(message, asserter.assertSameIndexOperation(replicatedOp, recoveredOp));
        assertTrue(message, asserter.assertSameIndexOperation(recoveredOp, replicatedOp));
    }

    /** Asserts that the asserter rejects the two sources as the same operation, in both argument orders. */
    private void assertDifferentOperation(String replicatedSource, String differentSource) throws IOException {
        Engine.IndexResult result = index(replicatedSource);
        Translog.Index replicatedOp = indexOp(result, replicatedSource);
        Translog.Index differentOp = indexOp(result, differentSource);
        TranslogOperationAsserter asserter = TranslogOperationAsserter.withEngineConfig(productionCodecConfig());

        String message = "replicated source: " + replicatedSource + " different source: " + differentSource;
        assertFalse(message, asserter.assertSameIndexOperation(replicatedOp, differentOp));
        assertFalse(message, asserter.assertSameIndexOperation(differentOp, replicatedOp));
    }

    private Engine.IndexResult index(String source) throws IOException {
        ParsedDocument doc = mapperService.documentMapper().parse(new SourceToParse("1", new BytesArray(source), XContentType.JSON));
        return engine.index(indexForDoc(doc));
    }

    private static Translog.Index indexOp(Engine.IndexResult result, String source) {
        return new Translog.Index("1", result.getSeqNo(), result.getTerm(), result.getVersion(), new BytesArray(source), null, -1);
    }

    /**
     * The engine config with a codec provider that always returns the production codec, built with the
     * mapper service so that vector fields get their own formats.
     */
    private EngineConfig productionCodecConfig() {
        CodecService codecService = new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE, null);
        return EngineConfig.builder(engine.config()).codecProvider(new CodecProvider() {
            @Override
            public Codec codec(String name) {
                return codecService.codec(CodecService.DEFAULT_CODEC);
            }

            @Override
            public String[] availableCodecs() {
                return codecService.availableCodecs();
            }
        }).build();
    }
}
