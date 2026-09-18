/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.misc.store.DirectIODirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfFlushConfigSource;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfMergeConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.es95.ES950DiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93BinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93FlatVectorFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * What a direct I/O merge leaves behind, which the context assertions in
 * {@link DirectIOCapableFlatVectorsFormatTests} do not observe: the merged raw vectors read back intact, and the
 * {@code on_disk_merge} flag recorded in the segment meta steering the merge after it. Neither varies with the format
 * above the raw vector format, so each case is one production path rather than one format: the data round trip once
 * per raw vectors writer, the meta once per meta implementation. A format that brings a raw writer or a meta
 * implementation no case here reaches needs one added.
 */
public class DirectIOMergeRoundTripTests extends BaseDirectIOMergeTestCase {

    /** only the data round trips need real direct I/O, so this gates them rather than the whole class */
    private static boolean directIOSupported;

    @BeforeClass
    public static void probeDirectIOSupport() throws IOException {
        Path path = createTempDir("directIOProbe");
        try (
            Directory dir = new FsDirectoryFactory.AlwaysDirectIODirectory(
                new MMapDirectory(path),
                DirectIODirectory.DEFAULT_MERGE_BUFFER_SIZE,
                DirectIODirectory.DEFAULT_MIN_BYTES_DIRECT,
                0
            );
            IndexOutput out = dir.createOutput("out", IOContext.DEFAULT)
        ) {
            out.writeString("test");
            directIOSupported = true;
        } catch (IOException | UnsupportedOperationException e) {
            directIOSupported = false;
        }
    }

    /**
     * The Lucene99 raw vectors writer, on the one format whose merge writes the vectors last, after clustering from a
     * temp file of its own, so the direct output is opened alongside other outputs on the same directory.
     */
    public void testMergedVectorsSurviveADirectIOMergeWrite() throws IOException {
        assertMergedVectorsSurviveADirectIOMergeWrite(bbqDisk(true));
    }

    /** The second raw vectors writer: bfloat16 has its own. */
    public void testBFloat16MergedVectorsSurviveADirectIOMergeWrite() throws IOException {
        assertMergedVectorsSurviveADirectIOMergeWrite(new ES93FlatVectorFormat(ElementType.BFLOAT16, true));
    }

    /**
     * The generic meta, written by a format whose searches read through the page cache: the reopened source takes
     * {@code fieldsReader}'s {@code else if (onDiskMerge)} branch, and the merge after it, whose sources recorded
     * nothing, takes the final {@code else}.
     */
    public void testTheFlagRoundTripsThroughTheGenericMeta() throws IOException {
        assertTheFlagRoundTripsThroughTheMeta(odm -> new ES93BinaryQuantizedVectorsFormat(ElementType.FLOAT, false, odm));
    }

    /** The second meta implementation: bbq_disk holds its raw vector format directly rather than through the generic wrapper. */
    public void testTheFlagRoundTripsThroughTheIVFMeta() throws IOException {
        assertTheFlagRoundTripsThroughTheMeta(DirectIOMergeRoundTripTests::bbqDisk);
    }

    private static KnnVectorsFormat bbqDisk(boolean onDiskMerge) {
        return new ES950DiskBBQVectorsFormat(
            QuantEncoding.ONE_BIT_4BIT_QUERY,
            64,
            ES950DiskBBQVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER,
            ElementType.FLOAT,
            false,
            null,
            1,
            false,
            ES950DiskBBQVectorsFormat.DEFAULT_PRECONDITIONING_BLOCK_DIMENSION,
            ES950DiskBBQVectorsFormat.defaultFlatThreshold(64),
            IvfFlushConfigSource.empty(),
            IvfMergeConfigResolver.useCodecDefault(),
            onDiskMerge
        );
    }

    private void assertMergedVectorsSurviveADirectIOMergeWrite(KnnVectorsFormat format) throws IOException {
        assumeTrue("needs a JDK and file system that support direct I/O, or the write falls back to the page cache", directIOSupported);
        int dims = 64;
        try (
            Directory dir = new FsDirectoryFactory.HybridDirectory(
                NativeFSLockFactory.INSTANCE,
                new MMapDirectory(createTempDir("directIOMergeData")),
                64
            );
            IndexWriter writer = new IndexWriter(dir, newConfig(format))
        ) {
            List<float[]> vectors = new ArrayList<>(addSegment(writer, dims));
            vectors.addAll(addSegment(writer, dims));
            mergeWithReaderOpen(writer);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                FloatVectorValues values = getOnlyLeafReader(reader).getFloatVectorValues("v");
                KnnVectorValues.DocIndexIterator iterator = values.iterator();
                List<float[]> unmatched = new ArrayList<>(vectors);
                while (iterator.nextDoc() != NO_MORE_DOCS) {
                    float[] candidate = values.vectorValue(iterator.index());
                    int match = 0;
                    while (match < unmatched.size() && sameVector(unmatched.get(match), candidate) == false) {
                        match++;
                    }
                    assertTrue("a merged vector matches no indexed vector that is still unmatched", match < unmatched.size());
                    unmatched.remove(match);
                }
                assertEquals("indexed vectors missing from the merged segment", 0, unmatched.size());
            }
        }
    }

    /** The tolerance covers bfloat16's 8-bit mantissa. */
    private static boolean sameVector(float[] vector, float[] candidate) {
        if (vector.length != candidate.length) {
            return false;
        }
        for (int i = 0; i < vector.length; i++) {
            if (Math.abs(vector[i] - candidate[i]) > 0.01f) {
                return false;
            }
        }
        return true;
    }

    /**
     * The flag travels with the segment. A segment written with {@code on_disk_merge} on records it in its meta. A
     * later merge reads that source with direct I/O whatever the mapping says by then, while its writes follow the
     * current mapping. The merged segment records that value, which the merge after it observes.
     */
    private void assertTheFlagRoundTripsThroughTheMeta(Function<Boolean, KnnVectorsFormat> formatWith) throws IOException {
        try (IORecordingDirectory dir = newRecordingDirectory(createTempDir("onDiskMergeMeta"))) {
            try (IndexWriter writer = new IndexWriter(dir, newConfig(formatWith.apply(true)))) {
                addSegment(writer, 64);
                addSegment(writer, 64);
            }
            dir.recorded.clear();
            try (IndexWriter writer = new IndexWriter(dir, newConfig(formatWith.apply(false)))) {
                mergeWithReaderOpen(writer);
                assertTrue(
                    "the sources were written with the flag on, so the merge must have read them with direct I/O",
                    dir.recorded.stream().anyMatch(io -> io.op() == Op.OPEN && io.mergeDirectIO())
                );
                assertTrue(
                    "the mapping is off, so the merge must not have written with direct I/O",
                    dir.recorded.stream().noneMatch(io -> io.op() == Op.CREATE && io.directIO())
                );
                addSegment(writer, 64);
                dir.recorded.clear();
                mergeWithReaderOpen(writer);
                assertTrue(
                    "no source of this merge recorded the flag, so nothing may use direct I/O in the merge",
                    dir.recorded.stream().noneMatch(FileIO::mergeDirectIO)
                );
            }
        }
    }
}
