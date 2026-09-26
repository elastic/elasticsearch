/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.util.TestUtil;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Shared assertions for the mixed-version scenario that the IVF (DiskBBQ / DiskASH) formats must survive: a node that
 * knows about the {@code on_disk_merge} byte in the per-field meta record reading, searching and merging segments written
 * by a node that did not. The meta layout is decided by the codec's meta version alone, so a segment written before the
 * byte existed must carry an older meta version than one written with it; if both are stamped with the same version, the
 * newer reader consumes the byte unconditionally, every following field shifts and opening the segment fails with
 * {@code CorruptIndexException: Invalid vector encoding id}.
 */
public final class IvfMetaVersionTestUtils {

    public static final String FIELD = "field";

    private IvfMetaVersionTestUtils() {}

    /**
     * Writes one segment with {@code oldWriter} (a format that stamps the meta version from before the
     * {@code on_disk_merge} byte) and one with {@code currentWriter}, checks that the meta files carry exactly the two
     * expected versions, reads every vector of both segments back, runs a kNN search over both, and finally force-merges
     * the two segments with the current codec (a merge reads the old segment through {@code getMergeInstance()}) and
     * checks the merged segment again.
     */
    public static void assertReadsSegmentsWrittenBeforeOnDiskMergeByte(
        Directory dir,
        KnnVectorsFormat oldWriter,
        KnnVectorsFormat currentWriter,
        String codecName,
        int expectedOldVersion,
        int expectedCurrentVersion,
        int dims,
        int docsPerSegment,
        Random random
    ) throws IOException {
        assertThat("the two writers must stamp different meta versions", expectedOldVersion, lessThan(expectedCurrentVersion));
        float[][] vectors = new float[2 * docsPerSegment][];
        for (int i = 0; i < vectors.length; i++) {
            vectors[i] = randomVector(random, dims);
        }

        // segment 1: written by the "old" node
        writeSegment(dir, oldWriter, vectors, 0, docsPerSegment);
        assertThat(metaVersions(dir, codecName), equalTo(new TreeSet<>(List.of(expectedOldVersion))));

        // segment 2: written by the "new" node into the same index
        writeSegment(dir, currentWriter, vectors, docsPerSegment, docsPerSegment);
        assertThat(metaVersions(dir, codecName), equalTo(new TreeSet<>(List.of(expectedOldVersion, expectedCurrentVersion))));

        // the new node opens both segments
        assertAllVectorsReadable(dir, vectors);
        assertSearchable(dir, vectors, random);

        // and merges them: the old segment is read through the merge reader of the new node
        IndexWriterConfig mergeConfig = new IndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(currentWriter))
            .setUseCompoundFile(false)
            .setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        try (IndexWriter w = new IndexWriter(dir, mergeConfig)) {
            w.forceMerge(1);
            w.commit();
        }
        assertThat(metaVersions(dir, codecName), equalTo(new TreeSet<>(List.of(expectedCurrentVersion))));
        assertAllVectorsReadable(dir, vectors);
        assertSearchable(dir, vectors, random);
    }

    private static void writeSegment(Directory dir, KnnVectorsFormat format, float[][] vectors, int from, int count) throws IOException {
        Codec codec = TestUtil.alwaysKnnVectorsFormat(format);
        IndexWriterConfig config = new IndexWriterConfig().setCodec(codec)
            .setUseCompoundFile(false)
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        try (IndexWriter w = new IndexWriter(dir, config)) {
            for (int i = from; i < from + count; i++) {
                Document doc = new Document();
                doc.add(new StringField("id", Integer.toString(i), Field.Store.YES));
                doc.add(new KnnFloatVectorField(FIELD, vectors[i], VectorSimilarityFunction.DOT_PRODUCT));
                w.addDocument(doc);
            }
            w.commit();
        }
    }

    /** The meta versions stamped on every IVF meta file in the directory. */
    public static TreeSet<Integer> metaVersions(Directory dir, String codecName) throws IOException {
        TreeSet<Integer> versions = new TreeSet<>();
        for (String file : dir.listAll()) {
            if (file.endsWith(".mivf") == false) {
                continue;
            }
            try (IndexInput in = dir.openInput(file, IOContext.READONCE)) {
                versions.add(CodecUtil.checkHeader(in, codecName, 0, Integer.MAX_VALUE));
            }
        }
        assertThat("no IVF meta file found in " + String.join(",", dir.listAll()), versions.size(), greaterThan(0));
        return versions;
    }

    private static void assertAllVectorsReadable(Directory dir, float[][] vectors) throws IOException {
        int seen = 0;
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals(vectors.length, reader.numDocs());
            for (LeafReaderContext ctx : reader.leaves()) {
                FloatVectorValues values = ctx.reader().getFloatVectorValues(FIELD);
                KnnVectorValues.DocIndexIterator it = values.iterator();
                for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                    int id = Integer.parseInt(ctx.reader().storedFields().document(doc).get("id"));
                    float[] actual = values.vectorValue(it.index());
                    assertThat("vector of doc " + id, actual, equalTo(vectors[id]));
                    seen++;
                }
            }
        }
        assertEquals("every vector of both segments must be readable", vectors.length, seen);
    }

    private static void assertSearchable(Directory dir, float[][] vectors, Random random) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            float[] query = vectors[random.nextInt(vectors.length)];
            TopDocs topDocs = searcher.search(new KnnFloatVectorQuery(FIELD, query, 10), 10);
            assertTrue("a search over both segments must return hits", topDocs.scoreDocs.length > 0);
        }
    }

    private static float[] randomVector(Random random, int dims) {
        float[] v = new float[dims];
        double norm = 0;
        for (int i = 0; i < dims; i++) {
            v[i] = random.nextFloat() * 2 - 1;
            norm += v[i] * v[i];
        }
        norm = Math.sqrt(norm);
        for (int i = 0; i < dims; i++) {
            v[i] /= (float) norm;
        }
        return v;
    }
}
