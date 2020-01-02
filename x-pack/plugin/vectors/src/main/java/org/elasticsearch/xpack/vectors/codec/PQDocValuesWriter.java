/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.vectors.codec;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper.MAX_PCENTROIDS_COUNT;
import static org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper.INT_BYTES;
import static org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper.KMEANS_ALGORITHM_LLOYDS;

public class PQDocValuesWriter extends DocValuesConsumer implements Closeable {
    private final SegmentWriteState state;
    private final DocValuesConsumer delegate;
    private final Random random;
    private final BigArrays bigArrays;
    private final int maxDoc;
    private IndexOutput data, meta;

    public PQDocValuesWriter(SegmentWriteState state, DocValuesConsumer delegate,
            String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
        this.state = state;
        this.delegate = delegate;

        this.random = new Random(42L);
        this.bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        this.maxDoc = state.segmentInfo.maxDoc();

        boolean success = false;
        try {
            String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
            data = state.directory.createOutput(dataName, state.context);
            CodecUtil.writeIndexHeader(data, dataCodec, PQDocValuesFormat.VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
            String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
            meta = state.directory.createOutput(metaName, state.context);
            CodecUtil.writeIndexHeader(meta, metaCodec, PQDocValuesFormat.VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        boolean success = false;
        try {
            if (meta != null) {
                meta.writeInt(-1); // write EOF marker
                CodecUtil.writeFooter(meta); // write checksum
            }
            if (data != null) {
                CodecUtil.writeFooter(data); // write checksum
            }
            success = true;
        } finally {
            if (success) {
                IOUtils.close(data, meta);
            } else {
                IOUtils.closeWhileHandlingException(data, meta);
            }
            meta = data = null;
        }
    }


    // TODO: redesign for cases where NOT all docs have vector values
    // TODO: handle deleted docs (don't write deleted docs)
    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        String ann = field.attributes().get("ann");
        if ((ann == null) || (ann.equals("pq") == false)) {
            delegate.addBinaryField(field, valuesProducer);
            return;
        }

        int dims = Integer.parseInt(field.attributes().get("dims"));
        int pqCount = Integer.parseInt(field.attributes().get("pqCount")); // number of product quantizers
        int kmeansAlgorithm = field.attributes().get("kmeansAlgorithm").equals(KMEANS_ALGORITHM_LLOYDS)? 1 : 2;
        int numIters = Integer.parseInt(field.attributes().get("kmeansIters"));
        float sampleFraction = Float.parseFloat(field.attributes().get("kmeansSampleFraction"));
        int pcentroidsCount = (int) Math.sqrt(maxDoc); // number of product centroids in each product quantizer
        if (pcentroidsCount > MAX_PCENTROIDS_COUNT) {
            pcentroidsCount = MAX_PCENTROIDS_COUNT;
        }

        int pdims = dims/pqCount;
        float[][][] pcentroids = new float[pqCount][pcentroidsCount][pdims];
        ByteArray[] docCentroids = new ByteArray[pqCount];
        DoubleArray[] docCentroidDists;
        if (kmeansAlgorithm == 1) {
            docCentroidDists = null;
        } else {
            docCentroidDists = new DoubleArray[pqCount];
        }
        KMeansClustering.runPQKMeansClustering(kmeansAlgorithm, valuesProducer, field, pcentroids, docCentroids,
            docCentroidDists, pcentroidsCount, pqCount, pdims, numIters, sampleFraction);

        meta.writeInt(field.number);
        meta.writeInt(dims); // number of vector dims
        meta.writeInt(pqCount); // number of product quantizers
        meta.writeInt(pcentroidsCount); // number of product centroids in each product quantizer

        // write original vectors
        long start = data.getFilePointer();
        meta.writeLong(start); // start of vectors
        BinaryDocValues values = valuesProducer.getBinary(field);
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            BytesRef v = values.binaryValue();
            data.writeBytes(v.bytes, v.offset, v.length);
        }
        meta.writeLong(data.getFilePointer() - start); // dataLength of vectors

        // write centroids
        start = data.getFilePointer();
        meta.writeLong(start); // start of centroids
        byte[] pcentroidBytes = new byte[pqCount * pcentroidsCount * pdims * INT_BYTES];
        ByteBuffer pcentroidByteBuffer = ByteBuffer.wrap(pcentroidBytes);
        float[][] pcSquaredMagnitudes = new float[pqCount][pcentroidsCount];
        for (int pq = 0; pq < pqCount; pq++) {
            for (int c = 0; c < pcentroidsCount; c++) {
                for (int dim = 0; dim < pdims; dim++) {
                    pcentroidByteBuffer.putFloat(pcentroids[pq][c][dim]);
                    pcSquaredMagnitudes[pq][c] += pcentroids[pq][c][dim] * pcentroids[pq][c][dim];
                }
            }
        }
        data.writeBytes(pcentroidBytes, pcentroidBytes.length);
        meta.writeInt((int) (data.getFilePointer() - start)); // dataLength of centroids

        // write centroids' squared magnitudes
        start = data.getFilePointer();
        meta.writeLong(start); // start of centroids' squared magnitudes
        byte[] pcSMdBytes = new byte[pqCount * pcentroidsCount * INT_BYTES];
        ByteBuffer pcSMByteBuffer = ByteBuffer.wrap(pcSMdBytes);
        for (int pq = 0; pq < pqCount; pq++) {
            for (int c = 0; c < pcentroidsCount; c++) {
                pcSMByteBuffer.putFloat(pcSquaredMagnitudes[pq][c]);
            }
        }
        data.writeBytes(pcSMdBytes, pcSMdBytes.length);
        meta.writeInt((int) (data.getFilePointer() - start)); // dataLength of centroids' squared magnitudes

        // write docCentroids -- which centroid each document belongs to
        start = data.getFilePointer();
        meta.writeLong(start); // start of centroids
        long numDocs = docCentroids[0].size();
        for (int i = 0; i < numDocs; i++) {
            for (int pq = 0; pq < pqCount; pq++) {
                data.writeByte(docCentroids[pq].get(i));
            }
        }
        meta.writeLong(data.getFilePointer() - start); // dataLength of docCentroids
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addNumericField(field, valuesProducer);
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedField(field, valuesProducer);
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedNumericField(field, valuesProducer);
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedSetField(field, valuesProducer);
    }
}
