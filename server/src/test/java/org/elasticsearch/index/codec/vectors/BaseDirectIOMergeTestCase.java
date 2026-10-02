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
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Shared fixture for the direct I/O merge tests. It provides a directory that records the {@link IOContext} of every
 * open and create, and the helpers those tests use to index two segments and merge them with a reader held open.
 */
abstract class BaseDirectIOMergeTestCase extends ESTestCase {

    enum Op {
        OPEN,
        CREATE
    }

    record FileIO(Op op, String name, IOContext.Context context, boolean directIO) {
        static FileIO of(Op op, String name, IOContext context) {
            return new FileIO(op, name, context.context(), context.hints().contains(DirectIOHint.INSTANCE));
        }

        boolean mergeDirectIO() {
            return context == IOContext.Context.MERGE && directIO;
        }
    }

    static class IORecordingDirectory extends FilterDirectory {
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

    static IORecordingDirectory newRecordingDirectory(Path path) throws IOException {
        return new IORecordingDirectory(new FsDirectoryFactory.HybridDirectory(NativeFSLockFactory.INSTANCE, new MMapDirectory(path), 64));
    }

    static IndexWriterConfig newConfig(KnnVectorsFormat format) {
        IndexWriterConfig config = new IndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(format));
        // direct I/O only applies to non-compound segments, and a .cfs would also hide the raw vector file opens
        config.setUseCompoundFile(false);
        config.getMergePolicy().setNoCFSRatio(0.0);
        return config;
    }

    static List<float[]> addSegment(IndexWriter writer, int dims) throws IOException {
        List<float[]> vectors = new ArrayList<>();
        int docs = randomIntBetween(30, 120);
        for (int i = 0; i < docs; i++) {
            float[] vector = BaseKnnVectorsFormatTestCase.randomNormalizedVector(dims);
            vectors.add(vector);
            Document doc = new Document();
            doc.add(new KnnFloatVectorField("v", vector, VectorSimilarityFunction.EUCLIDEAN));
            writer.addDocument(doc);
        }
        writer.commit();
        return vectors;
    }

    /** Merges the index down to one segment, with a reader held open across the merge. */
    static void mergeWithReaderOpen(IndexWriter writer) throws IOException {
        // holding a reader open makes the merge read through the pooled DEFAULT-context readers, as on a node serving
        // searches, rather than readers opened with a MERGE context, which do not use direct I/O
        try (DirectoryReader held = DirectoryReader.open(writer)) {
            assertTrue("the merge needs more than one segment", held.leaves().size() > 1);
            writer.forceMerge(1);
        }
        writer.commit();
    }
}
