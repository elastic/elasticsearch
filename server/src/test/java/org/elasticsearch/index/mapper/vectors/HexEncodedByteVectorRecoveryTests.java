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
     * In a mixed cluster the primary (running new code) produces this mixed source via replication,
     * while recovery from an old primary sends both fields as hex strings. Both clause 1 and clause 2
     * of the asserter fail for this case; only the third clause (comparing both synthesized forms) succeeds.
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
        // (the non-indexed field stayed as hex).
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
}
