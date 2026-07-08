/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.elasticsearch.nativeaccess.BBQTestUtils;
import org.elasticsearch.simdvec.BaseVectorizationTests;
import org.elasticsearch.simdvec.ES940OSQVectorsScorer;

import java.io.IOException;

import static org.elasticsearch.simdvec.internal.vectorization.JdkFeatures.SUPPORTS_HEAP_SEGMENTS;

public class MSBitToBitES940OSQVectorsScorerTests extends BaseVectorizationTests {

    public void testSymmetric1BitDotProductMatchesScalar() throws IOException {
        int dims = random().nextInt(1, 64) * 8;
        int length = BBQTestUtils.numBytes(dims, 1);

        byte[] unpackedQuery = new byte[dims];
        byte[] unpackedDoc = new byte[dims];
        for (int i = 0; i < dims; i++) {
            unpackedQuery[i] = (byte) random().nextInt(2);
            unpackedDoc[i] = (byte) random().nextInt(2);
        }
        byte[] query = BBQTestUtils.packStriped(unpackedQuery, 1);
        byte[] doc = BBQTestUtils.packStriped(unpackedDoc, 1);
        assertEquals(length, query.length);
        assertEquals(length, doc.length);

        long expected = 0;
        for (int i = 0; i < length; i++) {
            expected += Long.bitCount((query[i] & doc[i]) & 0xFF);
        }

        try (Directory dir = new MMapDirectory(createTempDir())) {
            try (IndexOutput out = dir.createOutput("v.bin", IOContext.DEFAULT)) {
                out.writeBytes(doc, 0, doc.length);
            }
            try (IndexInput in = dir.openInput("v.bin", IOContext.DEFAULT)) {
                var scalarScorer = new ES940OSQVectorsScorer(in, (byte) 1, (byte) 1, dims, length);
                assertEquals(expected, scalarScorer.quantizeScore(query));
            }
            try (IndexInput in2 = dir.openInput("v.bin", IOContext.DEFAULT)) {
                var panamaScorer = new MSBitToBitES940OSQVectorsScorer(in2, dims, length, ES940OSQVectorsScorer.BULK_SIZE);
                long panamaScore = panamaScorer.quantizeScore(query);
                if (panamaScore != Long.MIN_VALUE) {
                    assertEquals(expected, panamaScore);
                }
            }
        }
    }

    public void testSymmetric1BitDotProductMatchesNativeWhenSupported() throws IOException {
        assumeTrue("native scorer only supported on JDK 22+", SUPPORTS_HEAP_SEGMENTS);
        assumeTrue("native vector library not available", nativeProvider().getVectorScorerFactory().usesNative());
        int dims = random().nextInt(1, 64) * 8;
        int length = BBQTestUtils.numBytes(dims, 1);

        byte[] unpackedQuery = new byte[dims];
        byte[] unpackedDoc = new byte[dims];
        for (int i = 0; i < dims; i++) {
            unpackedQuery[i] = (byte) random().nextInt(2);
            unpackedDoc[i] = (byte) random().nextInt(2);
        }
        byte[] query = BBQTestUtils.packStriped(unpackedQuery, 1);
        byte[] doc = BBQTestUtils.packStriped(unpackedDoc, 1);

        long expected = 0;
        for (int i = 0; i < length; i++) {
            expected += Long.bitCount((query[i] & doc[i]) & 0xFF);
        }

        try (Directory dir = new MMapDirectory(createTempDir())) {
            try (IndexOutput out = dir.createOutput("v.bin", IOContext.DEFAULT)) {
                out.writeBytes(doc, 0, doc.length);
            }
            try (IndexInput in = dir.openInput("v.bin", IOContext.DEFAULT)) {
                var nativeScorer = MemorySegmentES940OSQVectorsScorer.usingNative(
                    in,
                    (byte) 1,
                    (byte) 1,
                    dims,
                    length,
                    ES940OSQVectorsScorer.BULK_SIZE,
                    ES940OSQVectorsScorer.BitEncoding.STRIPED
                );
                assertEquals(expected, nativeScorer.quantizeScore(query));
            }
        }
    }
}
