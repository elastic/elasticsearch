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
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.elasticsearch.core.IOUtils;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.index.codec.vectors.es93.ES93GenericHnswVectorsFormat.META_CODEC_NAME;
import static org.elasticsearch.index.codec.vectors.es93.ES93GenericHnswVectorsFormat.VECTOR_FORMAT_INFO_EXTENSION;
import static org.elasticsearch.index.codec.vectors.es93.ES93GenericHnswVectorsFormat.VERSION_CURRENT;
import static org.elasticsearch.index.codec.vectors.es93.ES93GenericHnswVectorsFormat.VERSION_START;

class ES93GenericFlatVectorsReader extends FlatVectorsReader {

    private final FlatVectorsReader vectorsReader;

    @FunctionalInterface
    interface GetFormatReader {
        FlatVectorsReader getReader(String formatName, boolean useDirectIO) throws IOException;
    }

    ES93GenericFlatVectorsReader(SegmentReadState state, GetFormatReader getFormatReader) throws IOException {
        super(null);    // Hacks ahoy!
        // read in the meta information
        final String metaFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            VECTOR_FORMAT_INFO_EXTENSION
        );
        int versionMeta = -1;
        FlatVectorsReader reader = null;
        try (var metaIn = state.directory.openChecksumInput(metaFileName)) {
            Throwable priorE = null;
            try {
                versionMeta = CodecUtil.checkIndexHeader(
                    metaIn,
                    META_CODEC_NAME,
                    VERSION_START,
                    VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                String innerFormatName = metaIn.readString();
                byte useDirectIO = metaIn.readByte();
                reader = getFormatReader.getReader(innerFormatName, useDirectIO == 1);
                if (reader == null) {
                    throw new IllegalStateException(
                        "Cannot find knn vector format [" + innerFormatName + "]" + (useDirectIO == 1 ? " with directIO" : "")
                    );
                }
            } catch (Throwable exception) {
                priorE = exception;
            } finally {
                CodecUtil.checkFooter(metaIn, priorE);
            }
            vectorsReader = reader;
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(this);
            throw t;
        }
    }

    @Override
    public FlatVectorsScorer getFlatVectorScorer() {
        return vectorsReader.getFlatVectorScorer();
    }

    @Override
    public FlatVectorsReader getMergeInstance() throws IOException {
        // we know what the reader is, so we can return it directly
        return vectorsReader.getMergeInstance();
    }

    @Override
    public void checkIntegrity() throws IOException {
        vectorsReader.checkIntegrity();
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        return vectorsReader.getFloatVectorValues(field);
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
        return vectorsReader.getByteVectorValues(field);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
        return vectorsReader.getRandomVectorScorer(field, target);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
        return vectorsReader.getRandomVectorScorer(field, target);
    }

    @Override
    public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        vectorsReader.search(field, target, knnCollector, acceptDocs);
    }

    @Override
    public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        vectorsReader.search(field, target, knnCollector, acceptDocs);
    }

    @Override
    public long ramBytesUsed() {
        return vectorsReader.ramBytesUsed();
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
        return vectorsReader.getOffHeapByteSize(fieldInfo);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(vectorsReader);
    }
}
