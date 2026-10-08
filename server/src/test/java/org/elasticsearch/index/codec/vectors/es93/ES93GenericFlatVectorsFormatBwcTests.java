/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * Segments written before {@link ES93GenericFlatVectorsFormat#VERSION_NO_DIRECT_IO} record each field's direct I/O options.
 * They are still read, and the options are skipped: how a file is read follows the mapping when it is opened. New segments do
 * not record them.
 */
public class ES93GenericFlatVectorsFormatBwcTests extends ESTestCase {

    private static final int DIMS = 8;
    private static final int DOCS = 16;

    public void testReadsSegmentsThatRecordTheDirectIOOptions() throws IOException {
        for (int version : new int[] { ES93GenericFlatVectorsFormat.VERSION_START, ES93GenericFlatVectorsFormat.VERSION_ON_DISK_MERGE }) {
            try (Directory dir = newDirectory()) {
                float[][] vectors = writeSegment(dir, version);
                assertEquals(version, metaVersion(dir));
                assertVectors(vectors, dir);
            }
        }
    }

    public void testNewSegmentsDoNotRecordThem() throws IOException {
        try (Directory dir = newDirectory()) {
            float[][] vectors = writeSegment(dir, ES93GenericFlatVectorsFormat.VERSION_CURRENT);
            assertEquals(ES93GenericFlatVectorsFormat.VERSION_NO_DIRECT_IO, metaVersion(dir));
            assertVectors(vectors, dir);
        }
    }

    private static float[][] writeSegment(Directory dir, int version) throws IOException {
        float[][] vectors = new float[DOCS][DIMS];
        ES93GenericFlatVectorsFormat format = new ES93GenericFlatVectorsFormat(ElementType.FLOAT, version);
        IndexWriterConfig iwc = new IndexWriterConfig().setCodec(new OneFormatCodec(format)).setUseCompoundFile(false);
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (float[] vector : vectors) {
                for (int d = 0; d < DIMS; d++) {
                    vector[d] = randomFloat();
                }
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("field", vector, VectorSimilarityFunction.EUCLIDEAN));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        return vectors;
    }

    private static int metaVersion(Directory dir) throws IOException {
        SegmentInfo info = SegmentInfos.readLatestCommit(dir).info(0).info;
        String meta = IndexFileNames.segmentFileName(info.name, "", ES93GenericFlatVectorsFormat.VECTOR_FORMAT_INFO_EXTENSION);
        try (IndexInput in = dir.openInput(meta, IOContext.READONCE)) {
            return CodecUtil.checkIndexHeader(
                in,
                ES93GenericFlatVectorsFormat.META_CODEC_NAME,
                ES93GenericFlatVectorsFormat.VERSION_START,
                ES93GenericFlatVectorsFormat.VERSION_CURRENT,
                info.getId(),
                ""
            );
        }
    }

    /** Reads the segment back with the current format. */
    private static void assertVectors(float[][] expected, Directory dir) throws IOException {
        SegmentInfo info = SegmentInfos.readLatestCommit(dir).info(0).info;
        FieldInfos fieldInfos = info.getCodec().fieldInfosFormat().read(dir, info, "", IOContext.DEFAULT);
        try (
            FlatVectorsReader reader = new ES93GenericFlatVectorsFormat().fieldsReader(
                new SegmentReadState(dir, info, fieldInfos, IOContext.DEFAULT)
            )
        ) {
            FloatVectorValues values = reader.getFloatVectorValues("field");
            assertEquals(expected.length, values.size());
            for (int ord = 0; ord < expected.length; ord++) {
                assertArrayEquals(expected[values.ordToDoc(ord)], values.vectorValue(ord), 0f);
            }
        }
    }

    /** Writes vectors with one format directly, so that a reader can be opened over the segment it wrote. */
    private static class OneFormatCodec extends FilterCodec {
        private final KnnVectorsFormat format;

        OneFormatCodec(KnnVectorsFormat format) {
            super(Codec.getDefault().getName(), Codec.getDefault());
            this.format = format;
        }

        @Override
        public KnnVectorsFormat knnVectorsFormat() {
            return format;
        }
    }
}
