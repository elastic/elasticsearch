/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es93;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.stream.Stream;

import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasEntry;

public class ES93FlatVectorFormatTests extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    private final DenseVectorFieldMapper.ElementType elementType;

    public ES93FlatVectorFormatTests(DenseVectorFieldMapper.ElementType elementType) {
        this.elementType = elementType;
    }

    @ParametersFactory
    public static Iterable<Object[]> elements() {
        return Stream.of(DenseVectorFieldMapper.ElementType.FLOAT, DenseVectorFieldMapper.ElementType.BYTE)
            .map(e -> new Object[] { e })
            .toList();
    }

    @Override
    protected VectorEncoding randomVectorEncoding() {
        return switch (elementType) {
            case FLOAT -> VectorEncoding.FLOAT32;
            case BYTE -> VectorEncoding.BYTE;
            default -> throw new IllegalArgumentException();
        };
    }

    @Override
    protected Codec getCodec() {
        return TestUtil.alwaysKnnVectorsFormat(new ES93FlatVectorFormat(elementType));
    }

    public void testSearchWithVisitedLimit() {
        throw new AssumptionViolatedException("requires graph-based vector codec");
    }

    public void testSimpleOffHeapSize() throws IOException {
        int vectorLength = random().nextInt(12, 500);
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
            Document doc = new Document();
            int byteSize = switch (elementType) {
                case FLOAT -> {
                    doc.add(new KnnFloatVectorField("f", randomVector(vectorLength), DOT_PRODUCT));
                    yield 4;
                }
                case BYTE -> {
                    doc.add(new KnnByteVectorField("f", randomVector8(vectorLength), DOT_PRODUCT));
                    yield 1;
                }
                default -> throw new IllegalArgumentException();
            };
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
                    assertThat(offHeap, aMapWithSize(1));
                    assertThat(offHeap, hasEntry("vec", (long) vectorLength * byteSize));
                }
            }
        }
    }
}
