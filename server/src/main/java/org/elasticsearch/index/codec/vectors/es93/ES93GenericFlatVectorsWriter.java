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
import org.elasticsearch.core.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class ES93GenericFlatVectorsWriter extends FlatVectorsWriter {

    private final String rawVectorFormatName;
    private final FlatVectorsWriter rawVectorWriter;
    private final IndexOutput metaOut;
    private final List<Integer> fieldNumbers = new ArrayList<>();
    private final int writeVersion;

    @SuppressWarnings("this-escape")
    ES93GenericFlatVectorsWriter(
        GenericFormatMetaInformation metaInfo,
        String rawVectorsFormatName,
        SegmentWriteState state,
        FlatVectorsWriter rawWriter
    ) throws IOException {
        this(metaInfo, rawVectorsFormatName, state, rawWriter, metaInfo.versionCurrent());
    }

    /** Writes the meta as {@code writeVersion} lays it out, so tests can write segments of earlier versions. */
    ES93GenericFlatVectorsWriter(
        GenericFormatMetaInformation metaInfo,
        String rawVectorsFormatName,
        SegmentWriteState state,
        FlatVectorsWriter rawWriter,
        int writeVersion
    ) throws IOException {
        super(rawWriter.getFlatVectorScorer());
        this.rawVectorFormatName = rawVectorsFormatName;
        this.rawVectorWriter = rawWriter;
        this.writeVersion = writeVersion;

        final String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaInfo.extension());
        try {
            this.metaOut = state.directory.createOutput(metaFileName, state.context);
            CodecUtil.writeIndexHeader(metaOut, metaInfo.codecName(), writeVersion, state.segmentInfo.getId(), state.segmentSuffix);
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(this);
            throw t;
        }
    }

    @Override
    public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
        var writer = rawVectorWriter.addField(fieldInfo);
        fieldNumbers.add(fieldInfo.number);
        return writer;
    }

    @Override
    public void mergeOneFlatVectorField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        rawVectorWriter.mergeOneFlatVectorField(fieldInfo, mergeState);
        writeMeta(fieldInfo.number);
    }

    @Override
    public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
        rawVectorWriter.flush(maxDoc, sortMap);

        for (Integer field : fieldNumbers) {
            writeMeta(field);
        }
    }

    private void writeMeta(int field) throws IOException {
        metaOut.writeInt(field);
        metaOut.writeString(rawVectorFormatName);
        // versions that record the direct I/O options write them as off; readers skip them
        if (writeVersion < ES93GenericFlatVectorsFormat.VERSION_NO_DIRECT_IO) {
            metaOut.writeByte((byte) 0);
            if (writeVersion >= ES93GenericFlatVectorsFormat.VERSION_ON_DISK_MERGE) {
                metaOut.writeByte((byte) 0);
            }
        }
    }

    @Override
    public void finish() throws IOException {
        rawVectorWriter.finish();

        if (metaOut != null) {
            metaOut.writeInt(-1);   // no more fields
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
