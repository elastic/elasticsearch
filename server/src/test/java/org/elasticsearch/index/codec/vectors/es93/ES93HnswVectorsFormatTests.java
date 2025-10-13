/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.SameThreadExecutorService;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.vectors.BFloat16;

import java.io.IOException;
import java.util.Locale;

import static java.lang.String.format;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class ES93HnswVectorsFormatTests extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    private KnnVectorsFormat format;

    protected boolean useBFloat16() {
        return false;
    }

    @Override
    public void setUp() throws Exception {
        format = new ES93HnswVectorsFormat(DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH, useBFloat16(), random().nextBoolean());
        super.setUp();
    }

    @Override
    protected Codec getCodec() {
        return TestUtil.alwaysKnnVectorsFormat(format);
    }

    public void testToString() {
        FilterCodec customCodec =
            new FilterCodec("foo", Codec.getDefault()) {
                @Override
                public KnnVectorsFormat knnVectorsFormat() {
                    return new ES93HnswVectorsFormat(10, 20, false, false);
                }
            };
        String expectedPattern =
            "ES93HnswVectorsFormat(name=ES93HnswVectorsFormat, maxConn=10, beamWidth=20," +
                " flatVectorFormat=ES93GenericFlatVectorsFormat(name=ES93GenericFlatVectorsFormat," +
                " format=Lucene99FlatVectorsFormat(name=Lucene99FlatVectorsFormat, flatVectorScorer=%s())))";
        var defaultScorer = format(Locale.ROOT, expectedPattern, "DefaultFlatVectorScorer");
        var memSegScorer =
            format(Locale.ROOT, expectedPattern, "Lucene99MemorySegmentFlatVectorsScorer");
        assertThat(customCodec.knnVectorsFormat().toString(), is(oneOf(defaultScorer, memSegScorer)));
    }

    public void testLimits() {
        expectThrows(IllegalArgumentException.class, () -> new ES93HnswVectorsFormat(-1, 20, false, false));
        expectThrows(IllegalArgumentException.class, () -> new ES93HnswVectorsFormat(0, 20, false, false));
        expectThrows(IllegalArgumentException.class, () -> new ES93HnswVectorsFormat(20, 0, false, false));
        expectThrows(IllegalArgumentException.class, () -> new ES93HnswVectorsFormat(20, -1, false, false));
        expectThrows(IllegalArgumentException.class, () -> new ES93HnswVectorsFormat(512 + 1, 20, false, false));
        expectThrows(IllegalArgumentException.class, () -> new ES93HnswVectorsFormat(20, 3201, false, false));
        expectThrows(
            IllegalArgumentException.class,
            () -> new ES93HnswVectorsFormat(20, 100, false, false, 1, new SameThreadExecutorService()));
    }

    public void testSimpleOffHeapSize() throws IOException {
        float[] vector = randomVector(random().nextInt(12, 500));
        try (Directory dir = newDirectory();
             IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
            Document doc = new Document();
            doc.add(new KnnFloatVectorField("f", vector, DOT_PRODUCT));
            w.addDocument(doc);
            w.commit();
            try (IndexReader reader = DirectoryReader.open(w)) {
                LeafReader r = getOnlyLeafReader(reader);
                if (r instanceof CodecReader codecReader) {
                    KnnVectorsReader knnVectorsReader = codecReader.getVectorReader();
                    if (knnVectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
                        knnVectorsReader = fieldsReader.getFieldReader("f");
                    }
                    var fieldInfo = r.getFieldInfos().fieldInfo("f");
                    var offHeap = knnVectorsReader.getOffHeapByteSize(fieldInfo);
                    int bytes = useBFloat16() ? BFloat16.BYTES : Float.BYTES;
                    assertEquals(vector.length * bytes, (long) offHeap.get("vec"));
                    assertEquals(1L, (long) offHeap.get("vex"));
                    assertEquals(2, offHeap.size());
                }
            }
        }
    }
}
