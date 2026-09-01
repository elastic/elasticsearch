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
import org.apache.lucene.index.LeafReader;
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
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.vectors.diskbbq.es95.ES950DiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.es818.DirectIOHint;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswBinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswVectorsFormat;
import org.elasticsearch.index.codec.vectors.es94.ES94HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Tests that formats based on {@link DirectIOCapableFlatVectorsFormat} open the raw vector data
 * with direct I/O for both searches and merges when direct I/O is requested, and that merges
 * create the merged raw vector data with direct I/O while flush-time writes and search-hot files
 * (quantized vectors, HNSW graph) stay buffered.
 */
public class DirectIOCapableFlatVectorsFormatTests extends LuceneTestCase {

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

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
            new ES94HnswScalarQuantizedVectorsFormat(16, 100, DenseVectorFieldMapper.ElementType.FLOAT, 7, true),
            true,
            true,
            false,
            true
        );
    }

    /** on_disk_rescore on, vector_merge on: direct I/O everywhere, the configuration we run. */
    public void testBbqHnswMergeReaderUsesMergeSizedDirectIO() throws IOException {
        runMergeTest(new ES93HnswBinaryQuantizedVectorsFormat(DenseVectorFieldMapper.ElementType.FLOAT, true), true, true, true, true);
    }

    /** on_disk_rescore off, vector_merge off: stock behaviour, nothing touches direct I/O. */
    public void testBbqHnswWithoutDirectIOStaysBuffered() throws IOException {
        runMergeTest(new ES93HnswBinaryQuantizedVectorsFormat(DenseVectorFieldMapper.ElementType.FLOAT, false), false, false, false, false);
    }

    /**
     * on_disk_rescore off, vector_merge on: rescoring keeps the page cache, merges bypass it. The
     * two decisions are independent, so the merge side must engage without the field asking for
     * direct I/O reads.
     */
    public void testBbqHnswDirectIOMergesWithoutOnDiskRescore() throws IOException {
        runMergeTest(new ES93HnswBinaryQuantizedVectorsFormat(DenseVectorFieldMapper.ElementType.FLOAT, false), true, false, true, true);
    }

    /** on_disk_rescore on, vector_merge off: direct I/O rescoring, merges through the page cache. */
    public void testBbqHnswOnDiskRescoreWithoutDirectIOMerges() throws IOException {
        runMergeTest(new ES93HnswBinaryQuantizedVectorsFormat(DenseVectorFieldMapper.ElementType.FLOAT, true), false, true, false, false);
    }

    /**
     * bbq_disk holds its raw vector format directly rather than through the generic wrapper, so it
     * is the format that would silently get the read side of the setting without the write side if
     * the two were not tied together at the raw format. With vector_merge on and no
     * on_disk_rescore, merges must read the sources and write the merged raw vectors with direct I/O.
     */
    public void testBbqDiskMergeUsesDirectIOReadsAndWrites() throws IOException {
        runMergeTest(
            new ES950DiskBBQVectorsFormat(64, ES950DiskBBQVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER),
            true,
            false,
            true,
            true
        );
    }

    /** bfloat16 has its own raw format and writer; with vector_merge on both sides must engage there too. */
    public void testBfloat16HnswMergeUsesDirectIOReadsAndWrites() throws IOException {
        runMergeTest(new ES93HnswVectorsFormat(16, 100, DenseVectorFieldMapper.ElementType.BFLOAT16), true, false, true, true);
    }

    private void runMergeTest(
        KnnVectorsFormat format,
        boolean directIOForVectorMerges,
        boolean expectDirectIOReads,
        boolean expectMergeHintedOpen,
        boolean expectDirectIOWrites
    ) throws IOException {
        int dims = 64;
        int docsPerSegment = 50;
        float[][] vectors = new float[docsPerSegment * 2][];
        for (int i = 0; i < vectors.length; i++) {
            vectors[i] = randomVector(dims);
        }

        Path path = createTempDir("directIOMerge");
        IndexWriterConfig config = new IndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(format));
        // direct I/O only applies to non-compound segments; compound files would also hide
        // the raw vector file opens behind the .cfs
        config.setUseCompoundFile(false);
        config.getMergePolicy().setNoCFSRatio(0.0);

        try (
            IORecordingDirectory dir = new IORecordingDirectory(
                new FsDirectoryFactory.HybridDirectory(NativeFSLockFactory.INSTANCE, new MMapDirectory(path), 64, directIOForVectorMerges)
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

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leafReader = getOnlyLeafReader(reader);
                FloatVectorValues values = leafReader.getFloatVectorValues("v");
                KnnVectorValues.DocIndexIterator iterator = values.iterator();
                int count = 0;
                while (iterator.nextDoc() != NO_MORE_DOCS) {
                    assertTrue(containsVector(vectors, values.vectorValue(iterator.index())));
                    count++;
                }
                assertEquals(vectors.length, count);

                TopDocs topDocs = new IndexSearcher(reader).search(new KnnFloatVectorQuery("v", vectors[0], 5), 5);
                assertEquals(5, topDocs.scoreDocs.length);
            }

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
                } else if (directIOForVectorMerges == false) {
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

            // direct IO writes are scoped to the raw vector data file and its metadata sibling;
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

    private static boolean containsVector(float[][] vectors, float[] candidate) {
        for (float[] vector : vectors) {
            if (sameVector(vector, candidate)) {
                return true;
            }
        }
        return false;
    }

    /** Exact for float32 formats; within bfloat16's 8-bit mantissa for the bfloat16 format. */
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

    private static float[] randomVector(int dims) {
        float[] vector = new float[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = random().nextFloat();
        }
        return vector;
    }
}
