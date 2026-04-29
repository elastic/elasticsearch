/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

public abstract class HnswKnnVectorsWriter extends KnnVectorsWriter {

    protected final SegmentWriteState segmentWriteState;
    private final FlatVectorsFormat flatVectorsFormat;
    protected final FlatVectorsWriter flatVectorWriter;
    protected FlatVectorsReader flatVectorsReader;
    private boolean flatWriterClosed;
    private boolean finished;

    protected HnswKnnVectorsWriter(
        SegmentWriteState segmentWriteState,
        FlatVectorsFormat flatVectorsFormat,
        FlatVectorsWriter flatVectorWriter
    ) {
        this.segmentWriteState = segmentWriteState;
        this.flatVectorsFormat = flatVectorsFormat;
        this.flatVectorWriter = flatVectorWriter;
    }

    protected void ensureFlatReaderOpen() throws IOException {
        if (flatVectorsReader == null) {
            flatVectorWriter.finish();
            flatVectorWriter.close();
            flatWriterClosed = true;
            SegmentReadState readState = new SegmentReadState(
                segmentWriteState.directory,
                segmentWriteState.segmentInfo,
                segmentWriteState.fieldInfos,
                segmentWriteState.context,
                segmentWriteState.segmentSuffix
            );
            flatVectorsReader = flatVectorsFormat.fieldsReader(readState);
        }
    }

    @Override
    public void finish() throws IOException {
        if (finished) {
            throw new IllegalStateException("already finished");
        }
        finished = true;
        if (flatWriterClosed == false) {
            flatVectorWriter.finish();
        }
    }
}
