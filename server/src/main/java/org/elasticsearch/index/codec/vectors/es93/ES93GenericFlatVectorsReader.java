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
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.vectors.GenericFlatVectorReaders;

import java.io.IOException;
import java.util.Map;

class ES93GenericFlatVectorsReader extends FlatVectorsReader {

    private final FieldInfos fieldInfos;
    private final GenericFlatVectorReaders genericReaders;

    ES93GenericFlatVectorsReader(
        GenericFormatMetaInformation metaInfo,
        SegmentReadState state,
        GenericFlatVectorReaders.LoadFlatVectorsReader loadReader
    ) throws IOException {
        super(null);    // this is not actually used by anything

        this.fieldInfos = state.fieldInfos;
        this.genericReaders = new GenericFlatVectorReaders();

        // read in the meta information
        final String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaInfo.extension());
        int versionMeta = -1;
        try (var metaIn = state.directory.openChecksumInput(metaFileName)) {
            Throwable priorE = null;
            try {
                versionMeta = CodecUtil.checkIndexHeader(
                    metaIn,
                    metaInfo.codecName(),
                    metaInfo.versionStart(),
                    metaInfo.versionCurrent(),
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );

                readFields(metaIn, state.fieldInfos, genericReaders, loadReader);
            } catch (Throwable exception) {
                priorE = exception;
            } finally {
                CodecUtil.checkFooter(metaIn, priorE);
            }
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(this);
            throw t;
        }
    }

    private ES93GenericFlatVectorsReader(FieldInfos fieldInfos, GenericFlatVectorReaders genericReaders) {
        super(null);
        this.fieldInfos = fieldInfos;
        this.genericReaders = genericReaders;
    }

    private static void readFields(
        IndexInput meta,
        FieldInfos fieldInfos,
        GenericFlatVectorReaders fieldHelper,
        GenericFlatVectorReaders.LoadFlatVectorsReader loadReader
    ) throws IOException {
        record FieldEntry(String rawVectorFormatName, boolean useDirectIOReads) implements GenericFlatVectorReaders.Field {}

        for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
            final FieldInfo info = fieldInfos.fieldInfo(fieldNumber);
            if (info == null) {
                throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
            }

            FieldEntry entry = new FieldEntry(meta.readString(), meta.readByte() == 1);
            fieldHelper.loadField(fieldNumber, entry, loadReader);
        }
    }

    @Override
    public FlatVectorsScorer getFlatVectorScorer() {
        // this should not actually be used at all
        return new FlatVectorsScorer() {
            @Override
            public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
                VectorSimilarityFunction similarityFunction,
                KnnVectorValues vectorValues
            ) throws IOException {
                throw new UnsupportedOperationException("Scorer should not be used");
            }

            @Override
            public RandomVectorScorer getRandomVectorScorer(
                VectorSimilarityFunction similarityFunction,
                KnnVectorValues vectorValues,
                float[] target
            ) throws IOException {
                throw new UnsupportedOperationException("Scorer should not be used");
            }

            @Override
            public RandomVectorScorer getRandomVectorScorer(
                VectorSimilarityFunction similarityFunction,
                KnnVectorValues vectorValues,
                byte[] target
            ) throws IOException {
                throw new UnsupportedOperationException("Scorer should not be used");
            }
        };
    }

    @Override
    public FlatVectorsReader getMergeInstance() throws IOException {
        return new ES93GenericFlatVectorsReader(fieldInfos, genericReaders.getMergeInstance());
    }

    @Override
    public void finishMerge() throws IOException {
        for (var reader : genericReaders.allReaders()) {
            reader.finishMerge();
        }
    }

    @Override
    public void checkIntegrity() throws IOException {
        for (var reader : genericReaders.allReaders()) {
            reader.checkIntegrity();
        }
    }

    private int findField(String field) {
        FieldInfo info = fieldInfos.fieldInfo(field);
        if (info == null) {
            throw new IllegalArgumentException("Could not find field [" + field + "]");
        }
        return info.number;
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        return genericReaders.getReaderForField(findField(field)).getFloatVectorValues(field);
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
        return genericReaders.getReaderForField(findField(field)).getByteVectorValues(field);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
        return genericReaders.getReaderForField(findField(field)).getRandomVectorScorer(field, target);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
        return genericReaders.getReaderForField(findField(field)).getRandomVectorScorer(field, target);
    }

    @Override
    public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        genericReaders.getReaderForField(findField(field)).search(field, target, knnCollector, acceptDocs);
    }

    @Override
    public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        genericReaders.getReaderForField(findField(field)).search(field, target, knnCollector, acceptDocs);
    }

    @Override
    public long ramBytesUsed() {
        return genericReaders.allReaders().stream().mapToLong(FlatVectorsReader::ramBytesUsed).sum();
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
        return genericReaders.getReaderForField(fieldInfo.number).getOffHeapByteSize(fieldInfo);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(genericReaders.allReaders());
    }
}
