/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.SetOnce;

import java.io.IOException;
import java.util.Objects;

public class ES920HnswComposableKnnVectorsFormat extends ComposablePerFieldKnnVectorsFormat.ComposableKnnVectorsFormat {

    private final SetOnce<FlatVectorsFormat> flatVectorsFormat;
    private final int maxConn;
    private final int beamWidth;

    public ES920HnswComposableKnnVectorsFormat() {
        this(null, 16, 100);
    }

    public ES920HnswComposableKnnVectorsFormat(FlatVectorsFormat flatVectorsFormat, int maxConn, int beamWidth) {
        super("ES920HnswComposableKnnVectorsFormat");
        this.flatVectorsFormat = new SetOnce<>();
        if (flatVectorsFormat != null) {
            this.flatVectorsFormat.set(flatVectorsFormat);
        }
        this.maxConn = maxConn;
        this.beamWidth = beamWidth;
    }

    @Override
    ComposablePerFieldKnnVectorsFormat.DirectoryModifier getDirectoryModifier() {
        return null;
    }

    @Override
    KnnVectorsFormat getInnerVectorsFormat() {
        return flatVectorsFormat.get();
    }

    @Override
    void setInnerVectorsFormat(KnnVectorsFormat format) {
        if (flatVectorsFormat.get() != null) {
            // allow it to be "set" again if it's the exact same format
            if (Objects.equals(flatVectorsFormat.get(), format)) {
                return;
            }
            throw new IllegalStateException("Inner format already set");
        }
        if (format instanceof FlatVectorsFormat fvf) {
            this.flatVectorsFormat.set(fvf);
            return;
        }
        throw new IllegalArgumentException("Inner format must be a FlatVectorsFormat, received: " + format.getClass());
    }

    @Override
    void setDirectoryModifier(ComposablePerFieldKnnVectorsFormat.DirectoryModifier modifier) {
        throw new UnsupportedOperationException("DirectoryModifier is not supported on HNSW format");
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        if (flatVectorsFormat.get() == null) {
            throw new IllegalStateException("flatVectorsFormat must be set");
        }
        return new Lucene99HnswVectorsWriter(state, maxConn, beamWidth, flatVectorsFormat.get().fieldsWriter(state), 1, null);
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        if (flatVectorsFormat.get() == null) {
            throw new IllegalStateException("flatVectorsFormat must be set");
        }
        return new Lucene99HnswVectorsReader(state, flatVectorsFormat.get().fieldsReader(state));
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return 4096;
    }
}
