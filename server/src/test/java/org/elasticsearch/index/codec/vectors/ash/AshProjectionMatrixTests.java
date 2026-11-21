/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests for {@link AshProjectionMatrix}.
 */
public class AshProjectionMatrixTests extends ESTestCase {

    public void testDimAccessors() {
        int originalDim = 768;
        int nDims = 384;
        float[] wT = new float[originalDim * nDims];
        AshProjectionMatrix pm = matrix(wT, originalDim, nDims, true);
        assertEquals(originalDim, pm.originalDim());
        assertEquals(nDims, pm.nDims());
    }

    public void testSerializationRoundtrip() throws IOException {
        int originalDim = randomIntBetween(4, 100);
        int nDims = randomIntBetween(2, originalDim);
        float[] wT = AshUtils.randomGaussians(random(), originalDim * nDims);

        AshProjectionMatrix original = matrix(wT, originalDim, nDims, true);

        AshProjectionMatrix restored = writeAndRead(original);

        assertEquals(originalDim, restored.originalDim());
        assertEquals(nDims, restored.nDims());
        assertArrayEquals(wT, restored.wT(), 0f);
    }

    public void testProjectQueryApproximatesFloatProjection() {
        int originalDim = randomIntBetween(64, 1024);
        int nDims = randomIntBetween(32, originalDim);
        float[] wT = AshUtils.randomGaussians(random(), originalDim * nDims);
        AshUtils.qrOrthogonalize(wT, originalDim, nDims);
        AshProjectionMatrix pm = matrix(wT, originalDim, nDims, true);
        float[] query = VectorTestUtils.randomFloatVector(originalDim);

        float[] expected = ESVectorUtil.matrixVectorMultiply(wT, nDims, originalDim, query);
        float[] projected = AshQueryProjection.quantize(pm.id(), wT, originalDim, nDims).projectQuery(query);

        double errorSq = 0;
        double expectedSq = 0;
        for (int j = 0; j < nDims; j++) {
            errorSq += Math.pow(projected[j] - expected[j], 2);
            expectedSq += Math.pow(expected[j], 2);
        }
        // int8 rounding of both the query and W^T, relative to the largest magnitude in each
        assertThat(Math.sqrt(errorSq / expectedSq), lessThan(0.03));
    }

    public void testProjectZeroQuery() throws IOException {
        int originalDim = randomIntBetween(4, 100);
        int nDims = randomIntBetween(2, originalDim);
        float[] wT = AshUtils.randomGaussians(random(), originalDim * nDims);
        AshProjectionMatrix pm = matrix(wT, originalDim, nDims, true);

        assertThat(AshProjectionMatrix.readQueryProjection(serialize(pm)).projectQuery(new float[originalDim]), equalTo(new float[nDims]));
    }

    public void testByteSizeMatchesActualSerialized() throws IOException {
        int originalDim = randomIntBetween(4, 50);
        int nDims = randomIntBetween(2, originalDim);
        float[] wT = AshUtils.randomGaussians(random(), originalDim * nDims);

        AshProjectionMatrix pm = matrix(wT, originalDim, nDims, true);

        ByteBuffersDataOutput dataOut = new ByteBuffersDataOutput();
        try (ByteBuffersIndexOutput out = new ByteBuffersIndexOutput(dataOut, "test", "test")) {
            pm.write(out);
        }

        // dims, learned flag, id, training sample size, then W^T
        long expectedSize = Integer.BYTES * 2L + Byte.BYTES + Long.BYTES * 2L + Integer.BYTES + (long) originalDim * nDims * Float.BYTES;
        assertEquals(expectedSize, pm.byteSize());
        assertEquals(pm.byteSize(), dataOut.size());
    }

    public void testLearnedFlagRoundTrips() throws IOException {
        int originalDim = randomIntBetween(4, 50);
        int nDims = randomIntBetween(2, originalDim);
        float[] wT = AshUtils.randomGaussians(random(), originalDim * nDims);

        for (boolean learned : new boolean[] { true, false }) {
            AshProjectionMatrix original = matrix(wT, originalDim, nDims, learned);
            assertEquals(learned, original.isLearned());
            AshProjectionMatrix restored = writeAndRead(original);
            assertEquals(learned, restored.isLearned());
            assertArrayEquals(wT, restored.wT(), 0f);
        }
    }

    public void testIdentityRoundTrips() throws IOException {
        int originalDim = randomIntBetween(4, 50);
        int nDims = randomIntBetween(2, originalDim);
        float[] wT = AshUtils.randomGaussians(random(), originalDim * nDims);
        UUID id = UUID.randomUUID();
        int trainingSampleSize = randomIntBetween(0, 100_000);

        AshProjectionMatrix original = AshProjectionMatrix.of(wT, originalDim, nDims, true, id, trainingSampleSize);
        AshProjectionMatrix restored = writeAndRead(original);
        assertEquals(id, restored.id());
        assertEquals(trainingSampleSize, restored.trainingSampleSize());
        assertEquals(originalDim, restored.originalDim());
        assertEquals(nDims, restored.nDims());
        assertArrayEquals(wT, restored.wT(), 0f);
    }

    public void testRejectsNegativeTrainingSampleSize() {
        expectThrows(IllegalArgumentException.class, () -> AshProjectionMatrix.of(new float[8], 4, 2, true, UUID.randomUUID(), -1));
    }

    public void testEmptyMatrix() throws IOException {
        AshProjectionMatrix pm = matrix(new float[0], 0, 0, true);
        assertEquals(0, pm.originalDim());
        assertEquals(0, pm.nDims());

        AshProjectionMatrix restored = writeAndRead(pm);
        assertEquals(0, restored.originalDim());
        assertEquals(0, restored.nDims());
    }

    public void testReadQueryProjectionQuantizesRowsAsTheyAreRead() throws IOException {
        int originalDim = randomIntBetween(4, 100);
        int nDims = randomIntBetween(2, originalDim);
        AshProjectionMatrix pm = matrix(AshUtils.randomGaussians(random(), originalDim * nDims), originalDim, nDims, randomBoolean());

        AshQueryProjection read = AshProjectionMatrix.readQueryProjection(serialize(pm));
        AshQueryProjection expected = AshQueryProjection.quantize(pm.id(), pm.wT(), originalDim, nDims);
        assertThat(read.id(), equalTo(pm.id()));
        assertThat(read.originalDim(), equalTo(originalDim));
        assertThat(read.nDims(), equalTo(nDims));
        float[] query = VectorTestUtils.randomFloatVector(originalDim);
        assertThat(read.projectQuery(query), equalTo(expected.projectQuery(query)));
    }

    public void testSeededStoresOnlyItsSeed() throws IOException {
        int originalDim = randomIntBetween(4, 100);
        int nDims = randomIntBetween(2, originalDim);
        AshProjectionMatrix seeded = seeded(originalDim, nDims, randomLong());
        assertThat(seeded.isSeeded(), equalTo(true));
        assertThat("nothing is learned into a seeded projection", seeded.isLearned(), equalTo(false));
        assertThat(seeded.trainingSampleSize(), equalTo(0));
        assertThat(serialize(seeded).length(), equalTo(seeded.byteSize()));
        assertThat("the header, then the seed", seeded.byteSize(), equalTo((long) headerSize() + Long.BYTES));

        AshProjectionMatrix restored = AshProjectionMatrix.read(serialize(seeded));
        assertThat(restored.isSeeded(), equalTo(true));
        assertThat(restored.isLearned(), equalTo(false));
        assertThat(restored.id(), equalTo(seeded.id()));
        assertThat(restored.wT(), equalTo(seeded.wT()));
    }

    public void testSeededIdDependsOnShapeAndSeedOnly() {
        int originalDim = randomIntBetween(4, 100);
        int nDims = randomIntBetween(3, originalDim);
        long seed = randomLong();
        UUID id = seeded(originalDim, nDims, seed).id();
        assertThat("a name-based UUID", id.version(), equalTo(3));
        assertThat(SeededProjections.id(originalDim, nDims, seed), equalTo(id));
        assertThat(seeded(originalDim, nDims, seed + 1).id(), not(equalTo(id)));
        assertThat(seeded(originalDim, nDims - 1, seed).id(), not(equalTo(id)));
    }

    public void testSeededProjectionIsSharedWhileInUse() throws IOException {
        int originalDim = randomIntBetween(4, 100);
        int nDims = randomIntBetween(2, originalDim);
        long seed = randomLong();
        AshProjectionMatrix seeded = seeded(originalDim, nDims, seed);
        assertThat(seeded(originalDim, nDims, seed).wT(), sameInstance(seeded.wT()));
        AshQueryProjection first = AshProjectionMatrix.readQueryProjection(serialize(seeded));
        assertThat(AshProjectionMatrix.readQueryProjection(serialize(seeded)), sameInstance(first));
        assertThat(first.id(), equalTo(seeded.id()));
        float[] query = VectorTestUtils.randomFloatVector(originalDim);
        assertThat(
            first.projectQuery(query),
            equalTo(AshQueryProjection.quantize(seeded.id(), seeded.wT(), originalDim, nDims).projectQuery(query))
        );
    }

    /**
     * Segments store only the seed of a seeded projection, so its generation is part of the format: this pins the
     * projection generated from a fixed seed, and fails if generation changes, which requires a new format version. The
     * entries are pinned within a tolerance, as vectorized arithmetic can round differently on different machines, far
     * below what a change to the algorithm would move them by. The id is pure arithmetic on the shape and seed, so it is
     * pinned exactly. The projection has more rows than {@link AshUtils#orthonormalizeRows} orthonormalizes in a block,
     * and entries are pinned from rows of both blocks, so that the rows projected off an earlier block are pinned too.
     */
    public void testSeededProjectionIsPinned() {
        int originalDim = 256;
        int nDims = 160;
        assertThat(nDims, greaterThan(AshUtils.ORTHONORMALIZE_BLOCK));
        AshProjectionMatrix seeded = seeded(originalDim, nDims, 42L);
        assertThat(seeded.id(), equalTo(UUID.fromString("08fd6b50-199a-38f9-a091-5369c5f2bde0")));
        float[] wT = seeded.wT();
        int[] entries = {
            0,
            1,
            255,
            256,
            5 * originalDim + 7,
            100 * originalDim + 40,
            127 * originalDim + 255,
            // the second block
            128 * originalDim,
            128 * originalDim + 3,
            140 * originalDim + 100,
            159 * originalDim + 255 };
        double[] expected = {
            0.066907406,
            0.053870667,
            0.08122442,
            -0.025851158,
            0.0443821,
            0.03149061,
            -0.006914098,
            -0.056652173,
            -0.042639498,
            -0.046136588,
            0.044607837 };
        for (int i = 0; i < entries.length; i++) {
            assertThat("entry " + entries[i], (double) wT[entries[i]], closeTo(expected[i], 1e-5));
        }
    }

    public void testRejectsLearnedAndSeeded() throws IOException {
        int originalDim = randomIntBetween(4, 50);
        int nDims = randomIntBetween(2, originalDim);
        byte[] bytes = bytes(matrix(AshUtils.randomGaussians(random(), originalDim * nDims), originalDim, nDims, randomBoolean()));
        // the flags follow the two dimensions: learned is 1, seeded is 2
        bytes[2 * Integer.BYTES] = 3;
        expectThrows(CorruptIndexException.class, () -> AshProjectionMatrix.readHeader(input(bytes)));
    }

    public void testRejectsUnknownFlags() throws IOException {
        int originalDim = randomIntBetween(4, 50);
        int nDims = randomIntBetween(2, originalDim);
        byte[] bytes = bytes(matrix(AshUtils.randomGaussians(random(), originalDim * nDims), originalDim, nDims, randomBoolean()));
        // the flags follow the two dimensions
        bytes[2 * Integer.BYTES] |= 4;
        expectThrows(CorruptIndexException.class, () -> AshProjectionMatrix.readHeader(input(bytes)));
    }

    public void testRejectsMismatchedSeededId() throws IOException {
        int originalDim = randomIntBetween(4, 50);
        int nDims = randomIntBetween(2, originalDim);
        long seed = randomLong();
        // the header of one seeded projection, with the seed of another
        byte[] bytes = bytes(seeded(originalDim, nDims, seed));
        byte[] otherSeed = bytes(seeded(originalDim, nDims, seed + 1));
        System.arraycopy(otherSeed, headerSize(), bytes, headerSize(), Long.BYTES);
        expectThrows(CorruptIndexException.class, () -> AshProjectionMatrix.read(input(bytes)));
        expectThrows(CorruptIndexException.class, () -> AshProjectionMatrix.readQueryProjection(input(bytes)));
    }

    public void testHeaderIsReadWithoutTheMatrix() throws IOException {
        int originalDim = randomIntBetween(4, 50);
        int nDims = randomIntBetween(2, originalDim);
        AshProjectionMatrix pm = AshProjectionMatrix.of(
            AshUtils.randomGaussians(random(), originalDim * nDims),
            originalDim,
            nDims,
            randomBoolean(),
            UUID.randomUUID(),
            randomIntBetween(0, 1000)
        );
        ByteBuffersIndexInput in = serialize(pm);
        AshProjectionMatrix.Header header = AshProjectionMatrix.readHeader(in);
        assertThat(
            header,
            equalTo(new AshProjectionMatrix.Header(originalDim, nDims, pm.isLearned(), false, pm.id(), pm.trainingSampleSize()))
        );
        assertThat("only the header was read", in.getFilePointer(), equalTo((long) headerSize()));
    }

    /** The size of the header of a serialized matrix, which the matrix or seed follows. */
    private static int headerSize() {
        return (int) (seeded(4, 2, 0L).byteSize() - Long.BYTES);
    }

    private static AshProjectionMatrix seeded(int originalDim, int nDims, long seed) {
        return AshProjectionMatrix.seeded(originalDim, nDims, seed, null);
    }

    /** A matrix with a new id and no recorded sample. */
    private static AshProjectionMatrix matrix(float[] wT, int originalDim, int nDims, boolean learned) {
        return AshProjectionMatrix.of(wT, originalDim, nDims, learned, UUID.randomUUID(), 0);
    }

    private AshProjectionMatrix writeAndRead(AshProjectionMatrix pm) throws IOException {
        return AshProjectionMatrix.read(serialize(pm));
    }

    private static ByteBuffersIndexInput serialize(AshProjectionMatrix pm) throws IOException {
        return input(bytes(pm));
    }

    private static byte[] bytes(AshProjectionMatrix pm) throws IOException {
        ByteBuffersDataOutput dataOut = new ByteBuffersDataOutput();
        try (ByteBuffersIndexOutput out = new ByteBuffersIndexOutput(dataOut, "test", "test")) {
            pm.write(out);
        }
        return dataOut.toArrayCopy();
    }

    private static ByteBuffersIndexInput input(byte[] bytes) {
        return new ByteBuffersIndexInput(new ByteBuffersDataInput(List.of(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN))), "test");
    }
}
