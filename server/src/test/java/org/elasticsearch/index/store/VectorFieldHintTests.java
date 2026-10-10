/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.index.codec.vectors.FieldKnnVectorsFormat;
import org.elasticsearch.index.codec.vectors.es94.ES94HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.Matchers.equalTo;

public class VectorFieldHintTests extends ESTestCase {

    /** Two fields written by distinct format instances take separate suffixes, so each resolves. */
    public void testResolvesTheFieldOfEachSuffix() throws Exception {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig();
            iwc.setCodec(new PerFieldPerInstanceCodec());
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("first", new float[] { 1, 0 }, VectorSimilarityFunction.DOT_PRODUCT));
                doc.add(new KnnFloatVectorField("second", new float[] { 0, 1 }, VectorSimilarityFunction.DOT_PRODUCT));
                w.addDocument(doc);
                w.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReaderContext leaf = reader.leaves().get(0);
                FieldInfos fieldInfos = leaf.reader().getFieldInfos();
                for (String field : List.of("first", "second")) {
                    String suffix = suffixOf(fieldInfos, field);
                    assertEquals(new VectorFieldHint(field), VectorFieldHint.forSuffix(fieldInfos, suffix));
                }
            }
        }
    }

    /** A suffix covering several fields describes none of them, so it resolves to nothing. */
    public void testSharedSuffixResolvesToNothing() throws Exception {
        try (Directory dir = newDirectory()) {
            // the default codec shares one format instance, so both fields land on one suffix
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("first", new float[] { 1, 0 }, VectorSimilarityFunction.DOT_PRODUCT));
                doc.add(new KnnFloatVectorField("second", new float[] { 0, 1 }, VectorSimilarityFunction.DOT_PRODUCT));
                w.addDocument(doc);
                w.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                FieldInfos fieldInfos = reader.leaves().get(0).reader().getFieldInfos();
                String shared = suffixOf(fieldInfos, "first");
                assertEquals(shared, suffixOf(fieldInfos, "second"));
                assertNull(VectorFieldHint.forSuffix(fieldInfos, shared));
            }
        }
    }

    public void testUnknownSuffixResolvesToNothing() throws Exception {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("first", new float[] { 1, 0 }, VectorSimilarityFunction.DOT_PRODUCT));
                w.addDocument(doc);
                w.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                FieldInfos fieldInfos = reader.leaves().get(0).reader().getFieldInfos();
                assertNull(VectorFieldHint.forSuffix(fieldInfos, "NoSuchFormat_7"));
            }
        }
    }

    private static String suffixOf(FieldInfos fieldInfos, String field) {
        var fi = fieldInfos.fieldInfo(field);
        return fi.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_FORMAT_KEY)
            + "_"
            + fi.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_SUFFIX_KEY);
    }

    /**
     * Hands every field its own format instance, the way Elasticsearch does for dense vectors. It
     * keeps the default codec's name so the segment still resolves to a registered codec on read;
     * the per-field suffixes it wrote are recorded in the field attributes either way.
     */
    private static class PerFieldPerInstanceCodec extends FilterCodec {
        PerFieldPerInstanceCodec() {
            super(Codec.getDefault().getName(), Codec.getDefault());
        }

        @Override
        public KnnVectorsFormat knnVectorsFormat() {
            return new PerFieldKnnVectorsFormat() {
                @Override
                public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                    // a new instance per field, which is what gives each its own files
                    return new Lucene99HnswVectorsFormat();
                }
            };
        }
    }

    /**
     * Two fields of one format get their per-field suffixes in a different order in each segment, and a merge numbers them
     * again: each merged raw vectors file still says the field it holds.
     */
    public void testAMergeSaysTheFieldOfEachMergedFile() throws Exception {
        List<IOContext> rawCreates = new CopyOnWriteArrayList<>();
        List<String> rawNames = new CopyOnWriteArrayList<>();
        Codec codec = new FilterCodec(Codec.getDefault().getName(), Codec.getDefault()) {
            @Override
            public KnnVectorsFormat knnVectorsFormat() {
                return new PerFieldKnnVectorsFormat() {
                    @Override
                    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                        return new FieldKnnVectorsFormat(field, new ES94HnswScalarQuantizedVectorsFormat());
                    }
                };
            }
        };
        try (Directory dir = new FilterDirectory(newDirectory()) {
            @Override
            public IndexOutput createOutput(String name, IOContext context) throws IOException {
                if (name.endsWith(".vec") && context.context() == IOContext.Context.MERGE) {
                    rawNames.add(name);
                    rawCreates.add(context);
                }
                return super.createOutput(name, context);
            }
        }) {
            IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec).setUseCompoundFile(false);
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                // the first segment meets b first, the second meets a first
                for (String first : List.of("b", "a")) {
                    String second = first.equals("a") ? "b" : "a";
                    for (int i = 0; i < 32; i++) {
                        Document doc = new Document();
                        doc.add(new KnnFloatVectorField(first, vector(), VectorSimilarityFunction.EUCLIDEAN));
                        if (i > 0) {
                            doc.add(new KnnFloatVectorField(second, vector(), VectorSimilarityFunction.EUCLIDEAN));
                        }
                        writer.addDocument(doc);
                    }
                    writer.flush();
                }
                writer.forceMerge(1);
            }
            SegmentCommitInfo merged = SegmentInfos.readLatestCommit(dir).info(0);
            FieldInfos fieldInfos = codec.fieldInfosFormat().read(dir, merged.info, "", IOContext.READONCE);
            assertThat(rawNames.size(), equalTo(2));
            for (String field : List.of("a", "b")) {
                FieldInfo info = fieldInfos.fieldInfo(field);
                String suffix = info.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_FORMAT_KEY)
                    + "_"
                    + info.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_SUFFIX_KEY);
                int created = -1;
                for (int i = 0; i < rawNames.size(); i++) {
                    if (rawNames.get(i).endsWith("_" + suffix + ".vec")) {
                        created = i;
                    }
                }
                assertTrue(field + " has no merged raw vectors among " + rawNames, created >= 0);
                assertThat(
                    field,
                    rawCreates.get(created).hints(VectorFieldHint.class).toList(),
                    equalTo(List.of(new VectorFieldHint(field)))
                );
            }
        }
    }

    private static float[] vector() {
        float[] v = new float[16];
        for (int i = 0; i < v.length; i++) {
            v[i] = randomFloat();
        }
        return v;
    }
}
