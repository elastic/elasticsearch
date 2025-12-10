/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;

import java.io.IOException;

public class ES813FlatRWVectorFormat extends ES813FlatVectorFormat {

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new ES813FlatVectorWriter(format.fieldsWriter(state));
    }

    static class ES813FlatVectorWriter extends KnnVectorsWriter {

        private final FlatVectorsWriter writer;

        ES813FlatVectorWriter(FlatVectorsWriter writer) {
            super();
            this.writer = writer;
        }

        @Override
        public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
            return writer.addField(fieldInfo);
        }

        @Override
        public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
            writer.flush(maxDoc, sortMap);
        }

        @Override
        public void finish() throws IOException {
            writer.finish();
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }

        @Override
        public long ramBytesUsed() {
            return writer.ramBytesUsed();
        }

        @Override
        public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
            writer.mergeOneField(fieldInfo, mergeState);
        }
    }

}
