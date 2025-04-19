/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.codec.vectors.reflect.OffHeapByteSizeUtils;
import org.junit.Before;

import java.io.IOException;

public class ES815HnswBitVectorsFormatTests extends BaseKnnBitVectorsFormatTestCase {

    @Override
    protected Codec getCodec() {
        return new Lucene101Codec() {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return new ES815HnswBitVectorsFormat();
            }
        };
    }

    @Before
    public void init() {
        similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
    }

    public void testSimpleOffHeapSize() throws IOException {
        byte[] vector = randomVector(random().nextInt(12, 500));
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
            Document doc = new Document();
            doc.add(new KnnByteVectorField("f", vector, VectorSimilarityFunction.EUCLIDEAN));
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
                    var offHeap = OffHeapByteSizeUtils.getOffHeapByteSize(knnVectorsReader, fieldInfo);
                    assertEquals(2, offHeap.size());
                    assertTrue(offHeap.get("vec") > 0L);
                    assertEquals(1L, (long) offHeap.get("vex"));
                }
            }
        }
    }
}
