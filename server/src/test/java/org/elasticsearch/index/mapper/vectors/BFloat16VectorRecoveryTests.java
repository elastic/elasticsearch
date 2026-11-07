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
 * Regression tests for mixed-version peer recovery with bfloat16 dense_vectors.
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

    /** Source sent by a 9.5 node during recovery, as logged by the failing 9.7 node: only vector7 is synthesized. */
    private static final String RECOVERED_SOURCE = """
        {"vector8":[130.0,115.0,-1.02,15.555,-100.0],"name":"cow.jpg",\
        "vector7":[229.90425,300.38586,-34.821266,15.522733,-199.69786]}""";

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
                }
              }
            }
            """;
    }

    public void testTranslogOperationAsserterAcceptsRecoveredSourceFromOlderNode() throws IOException {
        Engine.IndexResult result = indexOriginal();
        Translog.Index replicatedOp = indexOp(result, ORIGINAL_SOURCE);
        Translog.Index recoveredOp = indexOp(result, RECOVERED_SOURCE);

        assertTrue(
            "replicated source: " + ORIGINAL_SOURCE + " recovered source: " + RECOVERED_SOURCE,
            TranslogOperationAsserter.withEngineConfig(productionCodecConfig()).assertSameIndexOperation(replicatedOp, recoveredOp)
        );
    }

    public void testTranslogOperationAsserterRejectsDifferentVector() throws IOException {
        Engine.IndexResult result = indexOriginal();
        Translog.Index replicatedOp = indexOp(result, ORIGINAL_SOURCE);
        String differentSource = """
            {"vector8":[130.0,115.0,-1.02,15.555,-100.0],"name":"cow.jpg",\
            "vector7":[231.0,300.33,-34.8988,15.555,-200.0]}""";
        Translog.Index differentOp = indexOp(result, differentSource);

        assertFalse(
            "replicated source: " + ORIGINAL_SOURCE + " different source: " + differentSource,
            TranslogOperationAsserter.withEngineConfig(productionCodecConfig()).assertSameIndexOperation(replicatedOp, differentOp)
        );
    }

    private Engine.IndexResult indexOriginal() throws IOException {
        ParsedDocument doc = mapperService.documentMapper()
            .parse(new SourceToParse("1", new BytesArray(ORIGINAL_SOURCE), XContentType.JSON));
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
