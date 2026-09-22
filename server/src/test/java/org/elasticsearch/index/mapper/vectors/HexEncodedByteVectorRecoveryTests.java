/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.TranslogOperationAsserter;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Regression tests for mixed-version peer recovery with hex-encoded byte dense_vectors.
 *
 * <p>When an indexed byte dense_vector is stored in {@code _source} as a hex string (e.g.
 * {@code "807f0a"}) but is read back from the KNN vector index during
 * {@code LuceneChangesSnapshot} as a signed integer array (e.g. {@code [-128,127,10]}),
 * {@code TranslogWriter.assertNoSeqNumberConflict} must not throw an AssertionError because
 * the two representations encode the same vector.
 *
 * <p>For indices created at or after {@code IndexVersions.EXCLUDE_SOURCE_VECTORS_DEFAULT},
 * {@code index.mapping.source.mode.exclude_vectors} defaults to {@code true}. Both byte
 * dense_vector fields therefore appear in {@code MappingLookup.syntheticVectorFields()}, which
 * causes {@code TranslogOperationAsserter.withEngineConfig} to fall into the
 * {@code synthesizeSource} path. That path re-indexes the operation source into an in-memory
 * Lucene instance and reads it back via {@code LuceneChangesSnapshot}, which normalises all
 * byte vectors to signed integer arrays. The two normalised sources are then equal even though
 * the raw bytes differed.
 */
public class HexEncodedByteVectorRecoveryTests extends EngineTestCase {

    @Override
    protected String defaultMapping() {
        return """
            {
              "properties": {
                "my_vector_byte": {
                  "type": "dense_vector",
                  "element_type": "byte",
                  "dims": 3,
                  "index": false
                },
                "my_vector_byte_indexed": {
                  "type": "dense_vector",
                  "element_type": "byte",
                  "dims": 3,
                  "index": true,
                  "similarity": "l2_norm"
                }
              }
            }
            """;
    }

    /**
     * Verifies that {@code LuceneChangesSnapshot} (the peer recovery path) produces an operation
     * that the {@code TranslogOperationAsserter} considers equivalent to the original translog
     * operation, even when the byte vectors are normalised from hex strings to signed integer
     * arrays during the recovery read. The asserter uses {@code synthesizeSource} to re-normalise
     * the translog op before the JSON-map comparison.
     */
    public void testLuceneRecoveryOpIsEquivalentToOriginalTranslogOp() throws IOException {
        ParsedDocument doc = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "1",
                    new BytesArray("{\"my_vector_byte\":\"807f0a\",\"my_vector_byte_indexed\":\"807f0a\"}"),
                    XContentType.JSON
                )
            );
        assertNull(doc.dynamicMappingsUpdate());

        Engine.IndexResult result = engine.index(indexForDoc(doc));
        assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
        engine.flush();

        // Simulate peer recovery: read op back from the Lucene segment via LuceneChangesSnapshot.
        // For indexed byte vectors the KNN index returns signed integers, so the source may differ
        // from the hex-encoded original stored in _source.
        List<Translog.Operation> luceneOps = readAllOperationsInLucene(engine);
        assertThat(luceneOps, hasSize(1));
        assertThat(luceneOps.get(0), instanceOf(Translog.Index.class));
        Translog.Index luceneOp = (Translog.Index) luceneOps.get(0);

        // Reconstruct the original translog op (hex-encoded source, as the primary would have it).
        Translog.Index translogOp = new Translog.Index(
            result.getId(),
            result.getSeqNo(),
            result.getTerm(),
            result.getVersion(),
            doc.source().originalBytes(),
            null,
            -1
        );

        // TranslogWriter.assertNoSeqNumberConflict must not throw when these two ops are compared.
        TranslogOperationAsserter asserter = TranslogOperationAsserter.withEngineConfig(engine.config());
        assertTrue(
            "TranslogWriter.assertNoSeqNumberConflict must not fire for hex-vs-array byte vector representations;"
                + " translog source: "
                + translogOp.source().utf8ToString()
                + " lucene recovery source: "
                + luceneOp.source().utf8ToString(),
            asserter.assertSameIndexOperation(translogOp, luceneOp)
        );
    }

    /**
     * Verifies the exact production failure scenario: a "mixed-format" op where only the indexed
     * vector field was patched from the KNN index (as a signed integer array) while the non-indexed
     * field remained in its original hex-string form. In a mixed-version cluster, the primary
     * (running new code) produces this mixed source via replication, while recovery from an old
     * primary sends both fields as hex strings. Both clause 1 and clause 2 of the asserter fail
     * for this case; only the third clause (comparing both synthesized forms) succeeds.
     */
    public void testTranslogOperationAsserterAcceptsMixedAndHexAsEquivalent() throws IOException {
        ParsedDocument doc = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "1",
                    new BytesArray("{\"my_vector_byte\":\"807f0a\",\"my_vector_byte_indexed\":\"807f0a\"}"),
                    XContentType.JSON
                )
            );
        Engine.IndexResult primaryResult = engine.index(indexForDoc(doc));
        long seqNo = primaryResult.getSeqNo();
        long version = primaryResult.getVersion();
        long term = primaryResult.getTerm();

        // Simulates the op from replication from a new primary that patched only the indexed field
        // (the non-indexed field stayed as hex). This is the prvOp in the assertion failure.
        Translog.Index mixedOp = new Translog.Index(
            "1",
            seqNo,
            term,
            version,
            new BytesArray("{\"my_vector_byte\":\"807f0a\",\"my_vector_byte_indexed\":[-128,127,10]}"),
            null,
            -1
        );

        // Simulates the op from recovery from an old primary where both fields are hex strings.
        // This is the newOp in the assertion failure.
        Translog.Index allHexOp = new Translog.Index(
            "1",
            seqNo,
            term,
            version,
            new BytesArray("{\"my_vector_byte\":\"807f0a\",\"my_vector_byte_indexed\":\"807f0a\"}"),
            null,
            -1
        );

        // Without clause 3 (synthesized1 == synthesized2), both clause 1 and clause 2 fail for
        // this case: clause 1 compares synthesized(mixedOp) with allHexOp (not equal), and clause
        // 2 compares mixedOp with synthesized(allHexOp) (not equal because my_vector_byte differs
        // between hex and array forms in o1). Clause 3 normalises both to array form and matches.
        TranslogOperationAsserter asserter = TranslogOperationAsserter.withEngineConfig(engine.config());
        assertTrue(
            "TranslogOperationAsserter must accept a mixed-format op (hex non-indexed, array indexed) "
                + "against an all-hex op as equivalent; mixed source: "
                + mixedOp.source().utf8ToString()
                + " all-hex source: "
                + allHexOp.source().utf8ToString(),
            asserter.assertSameIndexOperation(mixedOp, allHexOp)
        );
    }

    /**
     * Directly verifies that {@code TranslogOperationAsserter} treats a hex-encoded byte vector
     * source and an integer-array-encoded byte vector source as equivalent for the same sequence
     * number, which is the condition that caused {@code TranslogWriter.assertNoSeqNumberConflict}
     * to fire in mixed-version clusters.
     *
     * <p>During peer recovery, {@code LuceneChangesSnapshot} normalises both indexed and
     * non-indexed byte dense_vector fields to signed integer arrays when
     * {@code exclude_source_vectors} is enabled (the default for indices created at or after
     * {@code IndexVersions.EXCLUDE_SOURCE_VECTORS_DEFAULT}). The asserter must therefore accept
     * an all-hex source (as written by the primary) against an all-array source (as produced by
     * the recovery snapshot). It does so by checking that {@code syntheticVectorFields} is
     * non-empty and then using {@code synthesizeSource} for a semantic (JSON-map) comparison.
     */
    public void testTranslogOperationAsserterAcceptsHexAndArrayAsEquivalent() throws IOException {
        ParsedDocument hexDoc = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "1",
                    new BytesArray("{\"my_vector_byte\":\"807f0a\",\"my_vector_byte_indexed\":\"807f0a\"}"),
                    XContentType.JSON
                )
            );
        Engine.IndexResult primaryResult = engine.index(indexForDoc(hexDoc));
        long seqNo = primaryResult.getSeqNo();
        long version = primaryResult.getVersion();

        // Hex-encoded source — what the primary writes to the translog.
        Translog.Index hexOp = new Translog.Index("1", seqNo, primaryResult.getTerm(), version, hexDoc.source().originalBytes(), null, -1);

        // Integer-array source — what LuceneChangesSnapshot produces during peer recovery.
        // Both the indexed and the non-indexed byte vector field are normalised to signed integer
        // arrays because syntheticVectorFields is non-empty for current-version indices.
        ParsedDocument arrayDoc = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "1",
                    new BytesArray("{\"my_vector_byte\":[-128,127,10],\"my_vector_byte_indexed\":[-128,127,10]}"),
                    XContentType.JSON
                )
            );
        Translog.Index arrayOp = new Translog.Index(
            "1",
            seqNo,
            primaryResult.getTerm(),
            version,
            arrayDoc.source().originalBytes(),
            null,
            -1
        );

        // For current-version indices (exclude_vectors defaults to true), both byte vector fields
        // are in syntheticVectorFields. The asserter then uses synthesizeSource for a semantic
        // JSON-map comparison rather than a raw byte comparison.
        var lookup = engine.config().getMapperService().mappingLookup();
        assertFalse(
            "syntheticVectorFields must be non-empty so the asserter does semantic comparison",
            lookup.syntheticVectorFields().isEmpty()
        );

        TranslogOperationAsserter asserter = TranslogOperationAsserter.withEngineConfig(engine.config());
        assertTrue(
            "TranslogOperationAsserter must treat hex-encoded and array-encoded byte vectors as equivalent;"
                + " hex source: "
                + hexOp.source().utf8ToString()
                + " array source: "
                + arrayOp.source().utf8ToString(),
            asserter.assertSameIndexOperation(hexOp, arrayOp)
        );
    }
}
