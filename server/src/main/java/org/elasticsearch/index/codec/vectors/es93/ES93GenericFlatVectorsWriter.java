/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.elasticsearch.core.IOUtils;

import java.io.IOException;

import static org.elasticsearch.index.codec.vectors.es93.ES93GenericHnswVectorsFormat.META_CODEC_NAME;
import static org.elasticsearch.index.codec.vectors.es93.ES93GenericHnswVectorsFormat.VECTOR_FORMAT_INFO_EXTENSION;
import static org.elasticsearch.index.codec.vectors.es93.ES93GenericHnswVectorsFormat.VERSION_CURRENT;

class ES93GenericFlatVectorsWriter extends FlatVectorsWriter {

    private final IndexOutput metaOut;
    private final FlatVectorsWriter rawVectorWriter;

    @SuppressWarnings("this-escape")
    ES93GenericFlatVectorsWriter(String knnFormatName, boolean useDirectIOReads, SegmentWriteState state, FlatVectorsWriter rawWriter)
        throws IOException {
        super(rawWriter.getFlatVectorScorer());
        this.rawVectorWriter = rawWriter;
        final String metaFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            VECTOR_FORMAT_INFO_EXTENSION
        );
        try {
            this.metaOut = state.directory.createOutput(metaFileName, state.context);
            CodecUtil.writeIndexHeader(metaOut, META_CODEC_NAME, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
            // write the format name used for this segment
            metaOut.writeString(knnFormatName);
            metaOut.writeByte(useDirectIOReads ? (byte) 1 : 0);
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(this);
            throw t;
        }
    }

    @Override
    public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
        return rawVectorWriter.addField(fieldInfo);
    }

    @Override
    public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        rawVectorWriter.mergeOneField(fieldInfo, mergeState);
    }

    @Override
    public CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        return rawVectorWriter.mergeOneFieldToIndex(fieldInfo, mergeState);
    }

    @Override
    public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
        rawVectorWriter.flush(maxDoc, sortMap);
    }

    @Override
    public void finish() throws IOException {
        rawVectorWriter.finish();

        if (metaOut != null) {
            CodecUtil.writeFooter(metaOut);
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(metaOut, rawVectorWriter);
    }

    @Override
    public long ramBytesUsed() {
        return rawVectorWriter.ramBytesUsed();
    }
}
