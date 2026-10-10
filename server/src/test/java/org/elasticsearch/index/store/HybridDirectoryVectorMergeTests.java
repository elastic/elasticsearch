/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.index.codec.vectors.FieldKnnVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.es95.ES950DiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93FlatVectorFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.lessThan;

/**
 * A merge on a {@link FsDirectoryFactory.HybridDirectory} whose mapping asks for {@code on_disk_merge}: the merged raw
 * vectors read back intact and, where the file system supports it, the merge wrote and read them with direct I/O. Each
 * case is one raw vectors writer.
 */
public class HybridDirectoryVectorMergeTests extends ESTestCase {

    private static final String FIELD = "vector";

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

    /** The Lucene99 raw vectors writer, on bbq_disk, whose merge also keeps a temp copy of the raw vectors. */
    public void testFloatVectorsSurviveADirectIOMerge() throws IOException {
        assertVectorsSurviveADirectIOMerge(
            new ES950DiskBBQVectorsFormat(64, ES950DiskBBQVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER)
        );
    }

    /** The bfloat16 raw vectors writer and reader. */
    public void testBFloat16VectorsSurviveADirectIOMerge() throws IOException {
        assertVectorsSurviveADirectIOMerge(new ES93FlatVectorFormat(ElementType.BFLOAT16));
    }

    private void assertVectorsSurviveADirectIOMerge(KnnVectorsFormat format) throws IOException {
        int dims = 64;
        try (
            DirectIORecordingDirectory dir = new DirectIORecordingDirectory(
                new FsDirectoryFactory.HybridDirectory(
                    NativeFSLockFactory.INSTANCE,
                    new MMapDirectory(createTempDir("directIOMerge")),
                    64,
                    () -> FsDirectoryFactoryTests.vectorField(false, true)
                )
            );
            IndexWriter writer = new IndexWriter(dir, newConfig(new FieldKnnVectorsFormat(FIELD, format)))
        ) {
            List<float[]> vectors = new ArrayList<>(addSegment(writer, dims));
            vectors.addAll(addSegment(writer, dims));
            dir.directCreates.clear();
            dir.directOpens.clear();
            writer.forceMerge(1);
            writer.commit();

            if (directIOSupported) {
                assertTrue("the merge wrote its raw vectors with direct I/O: " + dir.directCreates, hasVecFile(dir.directCreates));
                assertTrue("the merge read its raw vectors with direct I/O: " + dir.directOpens, hasVecFile(dir.directOpens));
            }

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                FloatVectorValues values = getOnlyLeafReader(reader).getFloatVectorValues(FIELD);
                KnnVectorValues.DocIndexIterator iterator = values.iterator();
                List<float[]> unmatched = new ArrayList<>(vectors);
                while (iterator.nextDoc() != NO_MORE_DOCS) {
                    float[] candidate = values.vectorValue(iterator.index());
                    int match = 0;
                    while (match < unmatched.size() && sameVector(unmatched.get(match), candidate) == false) {
                        match++;
                    }
                    assertThat("a merged vector matches no indexed vector that is still unmatched", match, lessThan(unmatched.size()));
                    unmatched.remove(match);
                }
                assertEquals("indexed vectors missing from the merged segment", 0, unmatched.size());
            }
        }
    }

    private static boolean hasVecFile(Set<String> names) {
        return names.stream().anyMatch(name -> name.endsWith(".vec"));
    }

    private static IndexWriterConfig newConfig(KnnVectorsFormat format) {
        IndexWriterConfig config = new IndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(format));
        // a compound file would hold the raw vectors, which direct I/O only applies to on their own
        config.setUseCompoundFile(false);
        config.getMergePolicy().setNoCFSRatio(0.0);
        return config;
    }

    private static List<float[]> addSegment(IndexWriter writer, int dims) throws IOException {
        List<float[]> vectors = new ArrayList<>();
        int docs = randomIntBetween(30, 120);
        for (int i = 0; i < docs; i++) {
            float[] vector = new float[dims];
            for (int d = 0; d < dims; d++) {
                vector[d] = randomFloat();
            }
            vectors.add(vector);
            Document doc = new Document();
            doc.add(new KnnFloatVectorField(FIELD, vector, VectorSimilarityFunction.EUCLIDEAN));
            writer.addDocument(doc);
        }
        writer.commit();
        return vectors;
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

    /** Records the files opened and created with direct I/O. */
    private static class DirectIORecordingDirectory extends FilterDirectory {
        final Set<String> directCreates = ConcurrentHashMap.newKeySet();
        final Set<String> directOpens = ConcurrentHashMap.newKeySet();

        DirectIORecordingDirectory(Directory in) {
            super(in);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            IndexInput in = super.openInput(name, context);
            if (in.getClass().getSimpleName().contains("DirectIOIndexInput")) {
                directOpens.add(name);
            }
            return in;
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) throws IOException {
            IndexOutput out = super.createOutput(name, context);
            if (out.toString().contains("DirectIOIndexOutput")) {
                directCreates.add(name);
            }
            return out;
        }
    }
}
