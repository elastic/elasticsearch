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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.misc.store.DirectIODirectory;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfFlushConfigSource;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfMergeConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.es95.ES950DiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.es818.DirectIOHint;
import org.elasticsearch.index.codec.vectors.es93.ES93BinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93FlatVectorFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswBinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswVectorsFormat;
import org.elasticsearch.index.codec.vectors.es94.ES94HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Tests that formats based on {@link DirectIOCapableFlatVectorsFormat} open the raw vector data
 * with direct I/O for searches and merges where the format engages it, and that merges
 * create the merged raw vector data with direct I/O where the format takes the write side, while
 * flush-time writes and search-hot files (quantized vectors, HNSW graph, plain HNSW's raw vectors)
 * stay buffered.
 */
public class DirectIOCapableFlatVectorsFormatTests extends ESTestCase {

    @BeforeClass
    public static void checkDirectIOSupported() throws IOException {
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
        } catch (IOException | UnsupportedOperationException e) {
            assumeNoException("test requires a JDK and filesystem that support Direct IO", e);
        }
    }

    private enum Op {
        OPEN,
        CREATE
    }

    /** Records the IOContext of every file open and create. */
    private record FileIO(Op op, String name, IOContext.Context context, boolean directIO, boolean mergeDirectIO) {
        static FileIO of(Op op, String name, IOContext context) {
            return new FileIO(
                op,
                name,
                context.context(),
                context.hints().contains(DirectIOHint.INSTANCE),
                context.context() == IOContext.Context.MERGE && context.hints().contains(DirectIOHint.INSTANCE)
            );
        }
    }

    private static class IORecordingDirectory extends FilterDirectory {
        final List<FileIO> recorded = new CopyOnWriteArrayList<>();

        IORecordingDirectory(Directory in) {
            super(in);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            recorded.add(FileIO.of(Op.OPEN, name, context));
            return super.openInput(name, context);
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) throws IOException {
            recorded.add(FileIO.of(Op.CREATE, name, context));
            return super.createOutput(name, context);
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
            IndexOutput out = super.createTempOutput(prefix, suffix, context);
            recorded.add(FileIO.of(Op.CREATE, out.getName(), context));
            return out;
        }
    }

    public void testInt8HnswOpensRawVectorsWithDirectIO() throws IOException {
        // the scalar-quantized reader chain does not currently propagate getMergeInstance down to
        // the raw reader, so merge-hinted opens cannot be asserted for this format; merge-time direct
        // I/O writes of the raw vectors do not depend on getMergeInstance, so they can be
        runMergeTest(
            new ES94HnswScalarQuantizedVectorsFormat(16, 100, DenseVectorFieldMapper.ElementType.FLOAT, 7, true, 1, null, 0, true),
            true,
            true,
            false,
            true
        );
    }

    /** on_disk_rescore on, on_disk_merge on: direct I/O everywhere, the configuration we run. */
    public void testBbqHnswMergeReaderUsesMergeSizedDirectIO() throws IOException {
        runMergeTest(
            new ES93HnswBinaryQuantizedVectorsFormat(16, 100, DenseVectorFieldMapper.ElementType.FLOAT, true, 1, null, 0, true),
            true,
            true,
            true,
            true
        );
    }

    /** on_disk_rescore off, on_disk_merge off: stock behaviour, nothing touches direct I/O. */
    public void testBbqHnswWithoutDirectIOStaysBuffered() throws IOException {
        runMergeTest(new ES93HnswBinaryQuantizedVectorsFormat(DenseVectorFieldMapper.ElementType.FLOAT, false), false, false, false, false);
    }

    /**
     * on_disk_rescore off, on_disk_merge on: rescoring keeps the page cache, merges bypass it. The
     * two decisions are independent, so the merge side must engage without the field asking for
     * direct I/O reads.
     */
    public void testBbqHnswDirectIOMergesWithoutOnDiskRescore() throws IOException {
        runMergeTest(
            new ES93HnswBinaryQuantizedVectorsFormat(16, 100, DenseVectorFieldMapper.ElementType.FLOAT, false, 1, null, 0, true),
            true,
            false,
            true,
            true
        );
    }

    /** on_disk_rescore on, on_disk_merge off: direct I/O rescoring, merges through the page cache. */
    public void testBbqHnswOnDiskRescoreWithoutDirectIOMerges() throws IOException {
        runMergeTest(new ES93HnswBinaryQuantizedVectorsFormat(DenseVectorFieldMapper.ElementType.FLOAT, true), false, true, false, false);
    }

    /**
     * bbq_disk holds its raw vector format directly rather than through the generic wrapper, so it
     * is the format that would silently get the read side without the write side if
     * the two were not tied together at the raw format. With on_disk_merge on and no
     * on_disk_rescore, merges must read the sources and write the merged raw vectors with direct I/O.
     */
    public void testBbqDiskMergeUsesDirectIOReadsAndWrites() throws IOException {
        runMergeTest(
            new ES950DiskBBQVectorsFormat(
                QuantEncoding.ONE_BIT_4BIT_QUERY,
                64,
                ES950DiskBBQVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER,
                DenseVectorFieldMapper.ElementType.FLOAT,
                false,
                null,
                1,
                false,
                ES950DiskBBQVectorsFormat.DEFAULT_PRECONDITIONING_BLOCK_DIMENSION,
                ES950DiskBBQVectorsFormat.defaultFlatThreshold(64),
                IvfFlushConfigSource.empty(),
                IvfMergeConfigResolver.useCodecDefault(),
                true
            ),
            true,
            false,
            true,
            true
        );
    }

    /**
     * Plain HNSW declines the write side: it builds the graph from the merged raw vectors by random
     * access right after writing them, so a direct write would only make that read-back cold. The
     * graph threshold of 0 makes the merge build a graph at this size (one merge worker, no
     * executor), so the read-back happens; merge reads of the sources still use direct I/O. Both
     * element types with their own raw writer.
     */
    public void testFloat32HnswMergeUsesDirectIOReadsAndBufferedWrites() throws IOException {
        runMergeTest(
            new ES93HnswVectorsFormat(16, 100, DenseVectorFieldMapper.ElementType.FLOAT, 1, null, 0, true),
            true,
            false,
            true,
            false
        );
    }

    public void testBfloat16HnswMergeUsesDirectIOReadsAndBufferedWrites() throws IOException {
        runMergeTest(
            new ES93HnswVectorsFormat(16, 100, DenseVectorFieldMapper.ElementType.BFLOAT16, 1, null, 0, true),
            true,
            false,
            true,
            false
        );
    }

    /**
     * The flat type's reader does not hand the merge a direct I/O reader (no getMergeInstance
     * override), so its merges read the sources through the page cache and write the merged raw
     * vectors with direct I/O. Both raw writers take the write side.
     */
    public void testFlatMergeReadsBufferedWritesDirect() throws IOException {
        runMergeTest(new ES93FlatVectorFormat(DenseVectorFieldMapper.ElementType.FLOAT, true), true, false, false, true);
    }

    public void testFlatBfloat16MergeReadsBufferedWritesDirect() throws IOException {
        runMergeTest(new ES93FlatVectorFormat(DenseVectorFieldMapper.ElementType.BFLOAT16, true), true, false, false, true);
    }

    /** bbq_flat's reader chain propagates getMergeInstance, so both sides engage. */
    public void testBbqFlatMergeUsesDirectIOReadsAndWrites() throws IOException {
        runMergeTest(new ES93BinaryQuantizedVectorsFormat(DenseVectorFieldMapper.ElementType.FLOAT, false, true), true, false, true, true);
    }

    /**
     * The flag travels with the segment: a segment written with {@code on_disk_merge} on records it on its field info,
     * a later merge reads that source with direct I/O whatever the mapping says by then, and the merged segment records
     * the flag the current mapping carries.
     */
    public void testOnDiskMergeIsRecordedPerSegmentAndFollowsTheCurrentMapping() throws IOException {
        int dims = 64;
        Path path = createTempDir("onDiskMergeAttribute");
        KnnVectorsFormat writtenWith = new ES93HnswVectorsFormat(16, 100, DenseVectorFieldMapper.ElementType.FLOAT, 1, null, 0, true);
        KnnVectorsFormat mergedWith = new ES93HnswVectorsFormat(16, 100, DenseVectorFieldMapper.ElementType.FLOAT, 1, null, 0, false);
        try (
            IORecordingDirectory dir = new IORecordingDirectory(
                new FsDirectoryFactory.HybridDirectory(NativeFSLockFactory.INSTANCE, new MMapDirectory(path), 64)
            )
        ) {
            IndexWriterConfig writeConfig = new IndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(writtenWith));
            writeConfig.setUseCompoundFile(false);
            writeConfig.getMergePolicy().setNoCFSRatio(0.0);
            try (IndexWriter writer = new IndexWriter(dir, writeConfig)) {
                for (int segment = 0; segment < 2; segment++) {
                    for (int i = 0; i < 20; i++) {
                        Document doc = new Document();
                        doc.add(
                            new KnnFloatVectorField(
                                "v",
                                BaseKnnVectorsFormatTestCase.randomNormalizedVector(dims),
                                VectorSimilarityFunction.EUCLIDEAN
                            )
                        );
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(2, reader.leaves().size());
                for (var leaf : reader.leaves()) {
                    assertEquals(
                        "true",
                        leaf.reader().getFieldInfos().fieldInfo("v").getAttribute(DirectIOCapableFlatVectorsFormat.ON_DISK_MERGE_ATTRIBUTE)
                    );
                }
            }
            dir.recorded.clear();
            // the mapping has been flipped off: the merge runs with a format that carries false
            IndexWriterConfig mergeConfig = new IndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(mergedWith));
            mergeConfig.setUseCompoundFile(false);
            mergeConfig.getMergePolicy().setNoCFSRatio(0.0);
            try (IndexWriter writer = new IndexWriter(dir, mergeConfig)) {
                try (DirectoryReader held = DirectoryReader.open(writer)) {
                    writer.forceMerge(1);
                }
                writer.commit();
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    assertEquals(
                        "false",
                        getOnlyLeafReader(reader).getFieldInfos()
                            .fieldInfo("v")
                            .getAttribute(DirectIOCapableFlatVectorsFormat.ON_DISK_MERGE_ATTRIBUTE)
                    );
                }
            }
            assertTrue(
                "the sources were written with the flag on, so the merge must have read them with direct I/O",
                dir.recorded.stream().anyMatch(io -> io.op() == Op.OPEN && io.name().endsWith(".vec") && io.directIO())
            );
        }
    }

    /**
     * @param onDiskMerge must match the {@code on_disk_merge} flag the format was built with: the format decides, this
     *                    only selects which expectations apply
     */
    private void runMergeTest(
        KnnVectorsFormat format,
        boolean onDiskMerge,
        boolean expectDirectIOReads,
        boolean expectMergeHintedOpen,
        boolean expectDirectIOWrites
    ) throws IOException {
        int dims = 64;
        int docsPerSegment = randomIntBetween(30, 120);
        float[][] vectors = new float[docsPerSegment * 2][];
        for (int i = 0; i < vectors.length; i++) {
            vectors[i] = BaseKnnVectorsFormatTestCase.randomNormalizedVector(dims);
        }

        Path path = createTempDir("directIOMerge");
        IndexWriterConfig config = new IndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(format));
        // direct I/O only applies to non-compound segments; compound files would also hide
        // the raw vector file opens behind the .cfs
        config.setUseCompoundFile(false);
        config.getMergePolicy().setNoCFSRatio(0.0);

        try (
            IORecordingDirectory dir = new IORecordingDirectory(
                new FsDirectoryFactory.HybridDirectory(NativeFSLockFactory.INSTANCE, new MMapDirectory(path), 64)
            );
            IndexWriter writer = new IndexWriter(dir, config)
        ) {
            for (int i = 0; i < vectors.length; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("v", vectors[i], VectorSimilarityFunction.EUCLIDEAN));
                writer.addDocument(doc);
                if (i == docsPerSegment - 1) {
                    writer.commit(); // ensure at least two segments exist, so forceMerge does real work
                }
            }
            writer.commit();

            // hold a reader open across the merge, so that the merge reads through the pooled
            // DEFAULT-context readers (as on a node serving searches) rather than readers opened
            // with a MERGE context, which do not use direct I/O
            try (DirectoryReader beforeMerge = DirectoryReader.open(writer)) {
                assertEquals(vectors.length, beforeMerge.numDocs());
                writer.forceMerge(1);
            }
            writer.commit();

            List<FileIO> vecOpens = dir.recorded.stream().filter(io -> io.op() == Op.OPEN && io.name().endsWith(".vec")).toList();
            assertTrue("expected at least one open of a raw vector file", vecOpens.isEmpty() == false);
            if (expectDirectIOReads) {
                assertTrue(
                    "expected the search-time reader to open the raw vector file with direct IO",
                    vecOpens.stream().anyMatch(o -> o.context() == IOContext.Context.DEFAULT && o.directIO())
                );
                if (expectMergeHintedOpen) {
                    for (FileIO open : vecOpens) {
                        if (open.context() == IOContext.Context.DEFAULT) {
                            assertTrue("raw vector file [" + open.name() + "] was opened without requesting direct IO", open.directIO());
                        }
                    }
                } else if (onDiskMerge == false) {
                    // direct I/O rescoring with page-cache merges: the merge must not borrow the
                    // random-access direct I/O reader, it reads the sources through a plain reader
                    // of its own, opened from the pooled (DEFAULT context) reader's state. The
                    // graph build's read-back of the merged output is a MERGE-context open and
                    // does not count.
                    assertTrue(
                        "expected the merge to open a source raw vector file through a plain (non direct IO) reader",
                        vecOpens.stream().anyMatch(o -> o.context() == IOContext.Context.DEFAULT && o.directIO() == false)
                    );
                }
            }
            if (expectMergeHintedOpen) {
                assertTrue(
                    "expected the merge to open a raw vector file with a merge-hinted direct IO context",
                    vecOpens.stream().anyMatch(FileIO::mergeDirectIO)
                );
            }

            List<FileIO> creates = dir.recorded.stream().filter(io -> io.op() == Op.CREATE).toList();

            // flush-time writes must never request direct IO: flush segments are small, are
            // searched immediately, and should stay page-cache-warm
            assertTrue(
                "a flush-time output was created with a direct IO hint",
                creates.stream().noneMatch(c -> c.context() == IOContext.Context.FLUSH && c.directIO())
            );

            // only the raw vector data file and its metadata sibling are even created with a direct IO hint
            // (the directory then keeps the metadata buffered);
            // quantized vectors, HNSW graph, format metadata and temp files must stay buffered so
            // they remain page-cache-warm after the merge
            assertTrue(
                "a file other than the raw vector data/meta was created with a direct IO hint",
                creates.stream().filter(FileIO::directIO).allMatch(c -> c.name().endsWith(".vec") || c.name().endsWith(".vemf"))
            );

            if (expectDirectIOWrites) {
                assertTrue(
                    "expected the merge to create the raw vector data file with a merge-hinted direct IO context",
                    creates.stream()
                        .anyMatch(
                            c -> c.name().endsWith(".vec") && c.context() == IOContext.Context.MERGE && c.directIO() && c.mergeDirectIO()
                        )
                );
            } else {
                assertTrue("did not expect any output to be created with a direct IO hint", creates.stream().noneMatch(FileIO::directIO));
            }
            if (expectDirectIOReads == false) {
                assertTrue(
                    "did not expect any search-time (DEFAULT context) open to request direct IO",
                    vecOpens.stream().noneMatch(o -> o.context() == IOContext.Context.DEFAULT && o.directIO())
                );
            }
            if (expectMergeHintedOpen == false) {
                assertTrue("did not expect any merge-hinted direct IO open", vecOpens.stream().noneMatch(FileIO::mergeDirectIO));
            }
            if (expectDirectIOReads == false && expectMergeHintedOpen == false && expectDirectIOWrites == false) {
                assertTrue(
                    "did not expect any direct IO opens or creates for this format",
                    dir.recorded.stream().noneMatch(FileIO::directIO)
                );
            }
        }
    }

    /**
     * A data-level check the context assertions above do not make: after a merge that wrote the merged
     * raw vectors with direct I/O, they read back intact and search. bfloat16 flat, whose raw writer
     * is ours; a tolerance of 0.01 covers its 8-bit mantissa.
     */
    public void testMergedVectorsSurviveDirectIOMergeWrites() throws IOException {
        int dims = 64;
        int docsPerSegment = randomIntBetween(30, 120);
        float[][] vectors = new float[docsPerSegment * 2][];
        for (int i = 0; i < vectors.length; i++) {
            vectors[i] = BaseKnnVectorsFormatTestCase.randomNormalizedVector(dims);
        }
        IndexWriterConfig config = new IndexWriterConfig().setCodec(
            TestUtil.alwaysKnnVectorsFormat(new ES93FlatVectorFormat(DenseVectorFieldMapper.ElementType.BFLOAT16, true))
        );
        config.setUseCompoundFile(false);
        config.getMergePolicy().setNoCFSRatio(0.0);
        try (
            Directory dir = new FsDirectoryFactory.HybridDirectory(
                NativeFSLockFactory.INSTANCE,
                new MMapDirectory(createTempDir("directIOMergeData")),
                64
            );
            IndexWriter writer = new IndexWriter(dir, config)
        ) {
            for (int i = 0; i < vectors.length; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("v", vectors[i], VectorSimilarityFunction.EUCLIDEAN));
                writer.addDocument(doc);
                if (i == docsPerSegment - 1) {
                    writer.commit();
                }
            }
            writer.commit();
            writer.forceMerge(1);
            writer.commit();
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                FloatVectorValues values = getOnlyLeafReader(reader).getFloatVectorValues("v");
                KnnVectorValues.DocIndexIterator iterator = values.iterator();
                int count = 0;
                while (iterator.nextDoc() != NO_MORE_DOCS) {
                    float[] candidate = values.vectorValue(iterator.index());
                    assertTrue(Arrays.stream(vectors).anyMatch(vector -> sameVector(vector, candidate)));
                    count++;
                }
                assertEquals(vectors.length, count);
                TopDocs topDocs = new IndexSearcher(reader).search(new KnnFloatVectorQuery("v", vectors[0], 5), 5);
                assertEquals(5, topDocs.scoreDocs.length);
            }
        }
    }

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

}
