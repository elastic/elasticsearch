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
import org.apache.lucene.codecs.KnnVectorsReader;
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
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.index.codec.vectors.BaseKnnBitVectorsFormatTestCase;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.junit.Before;

import java.io.IOException;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasToString;

public class ES93HnswBitVectorsFormatTests extends BaseKnnBitVectorsFormatTestCase {

    @Override
    protected Codec getCodec() {
        return TestUtil.alwaysKnnVectorsFormat(new ES93HnswVectorsFormat(DenseVectorFieldMapper.ElementType.BIT));
    }

    @Before
    public void init() {
        similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
    }

    public void testToString() {
        var format = new ES93HnswVectorsFormat(10, 20, DenseVectorFieldMapper.ElementType.BIT);
        assertThat(format, hasToString(containsString("name=ES93HnswVectorsFormat")));
        assertThat(format, hasToString(containsString("maxConn=10")));
        assertThat(format, hasToString(containsString("beamWidth=20")));
        assertThat(format, hasToString(containsString("hnswGraphThreshold=" + ES93HnswVectorsFormat.HNSW_GRAPH_THRESHOLD)));
    }

    public void testSimpleOffHeapSize() throws IOException {
        byte[] vector = randomVector(random().nextInt(12, 500));
        // Use threshold=0 to ensure HNSW graph is always built
        var format = new ES93HnswVectorsFormat(16, 100, DenseVectorFieldMapper.ElementType.BIT, 1, null, 0);
        var config = newIndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(format));
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, config)) {
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
                    var offHeap = knnVectorsReader.getOffHeapByteSize(fieldInfo);

                    assertThat(offHeap, aMapWithSize(2));
                    assertThat(offHeap, hasEntry("vex", 1L));
                    assertThat(offHeap, hasEntry(equalTo("vec"), greaterThan(0L)));
                }
            }
        }
    }
}
