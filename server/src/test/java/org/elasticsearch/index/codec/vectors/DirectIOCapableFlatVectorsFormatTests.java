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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.misc.store.DirectIODirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.index.codec.vectors.es818.DirectIOHint;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswBinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es94.ES94HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Tests that formats based on {@link DirectIOCapableFlatVectorsFormat} open the raw vector data
 * with direct I/O for both searches and merges when direct I/O is requested.
 */
public class DirectIOCapableFlatVectorsFormatTests extends ESTestCase {

    @BeforeClass
    public static void checkDirectIOSupported() throws IOException {
        Path path = createTempDir("directIOProbe");
        try (
            Directory dir = new FsDirectoryFactory.AlwaysDirectIODirectory(
                new MMapDirectory(path),
                FsDirectoryFactory.AlwaysDirectIODirectory.RANDOM_ACCESS_BUFFER_SIZE,
                DirectIODirectory.DEFAULT_MIN_BYTES_DIRECT,
                0
            )
        ) {
            try (IndexOutput out = dir.createOutput("out", IOContext.DEFAULT)) {
                out.writeString("test");
            }
            try (IndexInput in = dir.openInput("out", IOContext.DEFAULT)) {
                assertEquals("test", in.readString());
            }
        } catch (IOException | UnsupportedOperationException e) {
            assumeNoException("test requires a JDK and filesystem that support Direct IO", e);
        }
    }

    /** Records the IOContext of every open of a raw vector file. */
    private record VecOpen(String name, IOContext.Context context, boolean directIO, boolean mergeDirectIO) {}

    private static class VecOpenRecordingDirectory extends FilterDirectory {
        final List<VecOpen> vecOpens = new CopyOnWriteArrayList<>();

        VecOpenRecordingDirectory(Directory in) {
            super(in);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            if (name.endsWith(".vec")) {
                vecOpens.add(
                    new VecOpen(
                        name,
                        context.context(),
                        context.hints().contains(DirectIOHint.INSTANCE),
                        context.context() == IOContext.Context.MERGE && context.hints().contains(DirectIOHint.INSTANCE)
                    )
                );
            }
            return super.openInput(name, context);
        }
    }

    public void testInt8HnswOpensRawVectorsWithDirectIO() throws IOException {
        // the scalar-quantized reader chain does not currently propagate getMergeInstance down to
        // the raw reader, so only open-time direct I/O behavior can be asserted for this format
        int maxConn = 16;
        int beamWidth = 100;
        int bits = 7;
        boolean useDirectIO = true;
        runMergeTest(
            new ES94HnswScalarQuantizedVectorsFormat(maxConn, beamWidth, DenseVectorFieldMapper.ElementType.FLOAT, bits, useDirectIO),
            false
        );
    }

    public void testBbqHnswMergeReaderUsesMergeSizedDirectIO() throws IOException {
        // the binary-quantized reader chain propagates getMergeInstance to the raw reader, so the
        // merge triggers the lazy creation of the merge reader, which must open the raw vectors
        // with the merge hint
        runMergeTest(new ES93HnswBinaryQuantizedVectorsFormat(DenseVectorFieldMapper.ElementType.FLOAT, true), true);
    }

    private void runMergeTest(KnnVectorsFormat format, boolean expectMergeHintedOpen) throws IOException {
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
            VecOpenRecordingDirectory dir = new VecOpenRecordingDirectory(
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

            assertTrue("expected at least one open of a raw vector file", dir.vecOpens.isEmpty() == false);
            for (VecOpen open : dir.vecOpens) {
                if (open.context() == IOContext.Context.DEFAULT) {
                    assertTrue("raw vector file [" + open.name() + "] was opened without requesting direct IO", open.directIO());
                }
            }
            if (expectMergeHintedOpen) {
                assertTrue(
                    "expected the merge to open a raw vector file with a MERGE-context direct IO open",
                    dir.vecOpens.stream().anyMatch(VecOpen::mergeDirectIO)
                );
            } else {
                // documents the current gap: this chain does not propagate getMergeInstance, so no
                // MERGE-context open may occur; this fails loudly when propagation lands
                assertTrue(
                    "unexpected MERGE-context open for a chain that does not propagate getMergeInstance",
                    dir.vecOpens.stream().noneMatch(VecOpen::mergeDirectIO)
                );
            }
        }
    }

}
