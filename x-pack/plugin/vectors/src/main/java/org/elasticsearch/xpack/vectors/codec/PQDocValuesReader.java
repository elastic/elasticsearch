/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.vectors.codec;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper.INT_BYTES;

public class PQDocValuesReader extends DocValuesProducer implements Closeable {

    private final SegmentReadState state;
    private final DocValuesProducer delegate;
    private final IndexInput data;
    private final int maxDoc;
    private final Map<String, VectorBinaryEntry> vectorBinaries = new HashMap<>();

    public PQDocValuesReader(SegmentReadState state, DocValuesProducer delegate, String dataCodec, String dataExtension,
            String metaCodec, String metaExtension) throws IOException {
        this.state = state;
        this.delegate = delegate;
        this.maxDoc = state.segmentInfo.maxDoc();

        int version = -1;
        String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
        try (ChecksumIndexInput in = state.directory.openChecksumInput(metaName, state.context)) {
            Throwable exc = null;
            try {
                version = CodecUtil.checkIndexHeader(in, metaCodec, PQDocValuesFormat.VERSION_START, PQDocValuesFormat.VERSION_CURRENT,
                    state.segmentInfo.getId(), state.segmentSuffix);
                readFields(in, state.fieldInfos);
            } catch (Throwable exception) {
                exc = exception;
            } finally {
                CodecUtil.checkFooter(in, exc);
            }
        }
        String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
        this.data = state.directory.openInput(dataName, state.context);
        boolean success = false;
        try {
            int version2 = CodecUtil.checkIndexHeader(data, dataCodec, PQDocValuesFormat.VERSION_START, PQDocValuesFormat.VERSION_CURRENT,
                state.segmentInfo.getId(), state.segmentSuffix);
            if (version != version2) {
                throw new CorruptIndexException("Format versions mismatch: meta=" + version + ", data=" + version2, data);
            }
            CodecUtil.retrieveChecksum(data);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this.data);
            }
        }
    }

    private void readFields(ChecksumIndexInput meta, FieldInfos finfos) throws IOException {
        for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
            FieldInfo finfo = finfos.fieldInfo(fieldNumber);
            if (finfo == null) {
                throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
            }
            vectorBinaries.put(finfo.name, readVectorBinary(meta));
        }
    }

    private VectorBinaryEntry readVectorBinary(ChecksumIndexInput meta) throws IOException {
        VectorBinaryEntry entry = new VectorBinaryEntry();
        entry.dims = meta.readInt();
        entry.pqCount = meta.readInt();
        entry.pcentroidsCount = meta.readInt();
        entry.vectorsOffset = meta.readLong();
        entry.vectorsLength = meta.readLong();
        entry.centroidsOffset = meta.readLong();
        entry.centroidsLength = meta.readInt();
        entry.centroidsSquaredMagnitudeOffset = meta.readLong();
        entry.centroidsSquaredMagnitudeLength = meta.readInt();
        entry.docCentroidsOffset = meta.readLong();
        entry.docCentroidsLength = meta.readLong();
        return entry;
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        return delegate.getNumeric(field);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        String ann = field.attributes().get("ann");
        if ((ann == null) || (ann.equals("pq") == false)) {
            return delegate.getBinary(field);
        }

        VectorBinaryEntry entry = vectorBinaries.get(field.name);
        int pdims = entry.dims / entry.pqCount;

        final IndexInput vectorsSlice = data.slice("vectors", entry.vectorsOffset, entry.vectorsLength);
        final int vectorLength = entry.dims * INT_BYTES + INT_BYTES;  // vectors are encoded with their length

        final IndexInput docCentroidsSlice = data.slice("docCentroids", entry.docCentroidsOffset, entry.docCentroidsLength);
        final int centroidsLength = entry.pqCount;

        return new VectorDocValues(maxDoc) {
            final BytesRef vectorBytes = new BytesRef(new byte[vectorLength], 0, vectorLength);
            final BytesRef docCentroidsBytes = new BytesRef(new byte[centroidsLength], 0, centroidsLength);
            @Override
            public BytesRef binaryValue() throws IOException {
                vectorsSlice.seek((long) doc * vectorLength);
                vectorsSlice.readBytes(vectorBytes.bytes, 0, vectorLength);
                return vectorBytes;
            }

            @Override
            public BytesRef docCentroids() throws IOException {
                docCentroidsSlice.seek((long) doc * centroidsLength);
                docCentroidsSlice.readBytes(docCentroidsBytes.bytes, 0, centroidsLength);
                return docCentroidsBytes;
            }

            @Override
            public float[][][] pcentroids() throws IOException {
                byte[] pcentroidBytes = new byte[entry.centroidsLength];
                data.seek(entry.centroidsOffset);
                data.readBytes(pcentroidBytes, 0, entry.centroidsLength);

                ByteBuffer byteBuffer = ByteBuffer.wrap(pcentroidBytes, 0, entry.centroidsLength);
                float[][][] pcentroids = new float[entry.pqCount][entry.pcentroidsCount][pdims];
                for (int pq = 0; pq < entry.pqCount; pq++) {
                    for (int c = 0; c < entry.pcentroidsCount; c++) {
                        for (int dim = 0; dim < pdims; dim++) {
                            pcentroids[pq][c][dim] = byteBuffer.getFloat();
                        }
                    }
                }
                return pcentroids;
            }

            @Override
            public float[][] pCentroidsSquaredMagnitudes() throws IOException {
                byte[] pcSMdBytes = new byte[entry.centroidsSquaredMagnitudeLength];
                data.seek(entry.centroidsSquaredMagnitudeOffset);
                data.readBytes(pcSMdBytes, 0, entry.centroidsSquaredMagnitudeLength);

                ByteBuffer byteBuffer = ByteBuffer.wrap(pcSMdBytes, 0, entry.centroidsSquaredMagnitudeLength);
                float[][] pcSquaredMagnitudes = new float[entry.pqCount][entry.pcentroidsCount];
                for (int pq = 0; pq < entry.pqCount; pq++) {
                    for (int c = 0; c < entry.pcentroidsCount; c++) {
                        pcSquaredMagnitudes[pq][c] = byteBuffer.getFloat();
                    }
                }
                return pcSquaredMagnitudes;
            }

            @Override
            public int pcentroidsCount() {
                return entry.pcentroidsCount;
            }

        };
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        return delegate.getSorted(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        return delegate.getSortedNumeric(field);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        return delegate.getSortedSet(field);
    }

    @Override
    public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        data.close();
    }

    @Override
    public long ramBytesUsed() {
        return delegate.ramBytesUsed();
    }


    private static class VectorBinaryEntry {
        int dims;
        int pqCount;
        int pcentroidsCount;
        long vectorsOffset;
        long vectorsLength;
        long centroidsOffset;
        int centroidsLength;
        long centroidsSquaredMagnitudeOffset;
        int centroidsSquaredMagnitudeLength;
        long docCentroidsOffset;
        long docCentroidsLength;
    }
}
