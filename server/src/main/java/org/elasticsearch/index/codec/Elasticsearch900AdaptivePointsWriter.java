/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.MutablePointTree;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.lucene90.Lucene90PointsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.bkd.BKDConfig;
import org.apache.lucene.util.bkd.BKDWriter;
import org.elasticsearch.core.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link PointsWriter} that adaptively increases {@code maxPointsInLeafNode} for large fields
 * to bound the heap memory used by BKD tree metadata structures during indexing.
 */
class Elasticsearch900AdaptivePointsWriter extends PointsWriter {

    /**
     * Target maximum heap usage (in bytes) for BKD tree metadata structures during indexing.
     * When the estimated heap for a field exceeds this target, maxPointsInLeafNode is increased.
     */
    static final long TARGET_MAX_BKD_HEAP_BYTES = 150L * 1024 * 1024;

    /**
     * Estimated per-leaf heap overhead (excluding splitPackedValues) in bytes. Accounts for the
     * packed index byte[] object headers and ArrayList backing array entries, splitDimensionValues,
     * and leafBlockFPs.
     */
    static final int ESTIMATED_OVERHEAD_PER_LEAF_EXCLUDING_SPLIT_VALUES = 62;

    /** Upper bound on the adaptive maxPointsInLeafNode adjustment. */
    static final int MAX_POINTS_IN_LEAF_NODE_UPPER_BOUND = 4096;

    // File extensions and codec names must match Lucene90PointsFormat for read compatibility
    private static final String DATA_CODEC_NAME = "Lucene90PointsFormatData";
    private static final String INDEX_CODEC_NAME = "Lucene90PointsFormatIndex";
    private static final String META_CODEC_NAME = "Lucene90PointsFormatMeta";
    static final String DATA_EXTENSION = "kdd";
    static final String INDEX_EXTENSION = "kdi";
    static final String META_EXTENSION = "kdm";
    private static final int VERSION_CURRENT = 1;

    private final IndexOutput metaOut, indexOut, dataOut;
    private final SegmentWriteState writeState;
    private final int maxPointsInLeafNode;
    private final double maxMBSortInHeap;
    private boolean finished;

    Elasticsearch900AdaptivePointsWriter(SegmentWriteState writeState) throws IOException {
        this(writeState, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE, BKDWriter.DEFAULT_MAX_MB_SORT_IN_HEAP);
    }

    Elasticsearch900AdaptivePointsWriter(SegmentWriteState writeState, int maxPointsInLeafNode, double maxMBSortInHeap) throws IOException {
        assert writeState.fieldInfos.hasPointValues();
        this.writeState = writeState;
        this.maxPointsInLeafNode = maxPointsInLeafNode;
        this.maxMBSortInHeap = maxMBSortInHeap;

        String dataFileName = IndexFileNames.segmentFileName(writeState.segmentInfo.name, writeState.segmentSuffix, DATA_EXTENSION);
        dataOut = writeState.directory.createOutput(dataFileName, writeState.context);
        try {
            CodecUtil.writeIndexHeader(dataOut, DATA_CODEC_NAME, VERSION_CURRENT, writeState.segmentInfo.getId(), writeState.segmentSuffix);

            String metaFileName = IndexFileNames.segmentFileName(writeState.segmentInfo.name, writeState.segmentSuffix, META_EXTENSION);
            metaOut = writeState.directory.createOutput(metaFileName, writeState.context);
            CodecUtil.writeIndexHeader(metaOut, META_CODEC_NAME, VERSION_CURRENT, writeState.segmentInfo.getId(), writeState.segmentSuffix);

            String indexFileName = IndexFileNames.segmentFileName(writeState.segmentInfo.name, writeState.segmentSuffix, INDEX_EXTENSION);
            indexOut = writeState.directory.createOutput(indexFileName, writeState.context);
            CodecUtil.writeIndexHeader(
                indexOut,
                INDEX_CODEC_NAME,
                VERSION_CURRENT,
                writeState.segmentInfo.getId(),
                writeState.segmentSuffix
            );
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(this);
            throw t;
        }
    }

    @Override
    public void writeField(FieldInfo fieldInfo, PointsReader reader) throws IOException {
        PointValues.PointTree values = reader.getValues(fieldInfo.name).getPointTree();

        // XXX This is the point of difference from the standard Lucene90PointsWriter XXX
        int effectiveMaxPoints = adjustMaxPointsInLeafNode(maxPointsInLeafNode, fieldInfo.getPointNumBytes(), values.size());
        BKDConfig config = new BKDConfig(
            fieldInfo.getPointDimensionCount(),
            fieldInfo.getPointIndexDimensionCount(),
            fieldInfo.getPointNumBytes(),
            effectiveMaxPoints
        );

        try (
            BKDWriter writer = new BKDWriter(
                writeState.segmentInfo.maxDoc(),
                writeState.directory,
                writeState.segmentInfo.name,
                config,
                maxMBSortInHeap,
                values.size()
            )
        ) {
            if (values instanceof MutablePointTree mpt) {
                IORunnable finalizer = writer.writeField(metaOut, indexOut, dataOut, fieldInfo.name, mpt);
                if (finalizer != null) {
                    metaOut.writeInt(fieldInfo.number);
                    finalizer.run();
                }
                return;
            }

            values.visitDocValues(new IntersectVisitor() {
                @Override
                public void visit(int docID) {
                    throw new IllegalStateException();
                }

                @Override
                public void visit(int docID, byte[] packedValue) throws IOException {
                    writer.add(packedValue, docID);
                }

                @Override
                public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                    return Relation.CELL_CROSSES_QUERY;
                }
            });

            IORunnable finalizer = writer.finish(metaOut, indexOut, dataOut);
            if (finalizer != null) {
                metaOut.writeInt(fieldInfo.number);
                finalizer.run();
            }
        }
    }

    @Override
    public void merge(MergeState mergeState) throws IOException {
        for (PointsReader reader : mergeState.pointsReaders) {
            if (reader instanceof Lucene90PointsReader == false) {
                super.merge(mergeState);
                return;
            }
        }
        for (PointsReader reader : mergeState.pointsReaders) {
            if (reader != null) {
                reader.checkIntegrity();
            }
        }

        for (FieldInfo fieldInfo : mergeState.mergeFieldInfos) {
            if (fieldInfo.getPointDimensionCount() != 0) {
                if (fieldInfo.getPointDimensionCount() == 1) {
                    long totMaxSize = 0;
                    for (int i = 0; i < mergeState.pointsReaders.length; i++) {
                        PointsReader reader = mergeState.pointsReaders[i];
                        if (reader != null) {
                            FieldInfos readerFieldInfos = mergeState.fieldInfos[i];
                            FieldInfo readerFieldInfo = readerFieldInfos.fieldInfo(fieldInfo.name);
                            if (readerFieldInfo != null && readerFieldInfo.getPointDimensionCount() > 0) {
                                PointValues values = reader.getValues(fieldInfo.name);
                                if (values != null) {
                                    totMaxSize += values.size();
                                }
                            }
                        }
                    }

                    // XXX This is the point of difference from the standard Lucene90PointsWriter XXX
                    int effectiveMaxPoints = adjustMaxPointsInLeafNode(maxPointsInLeafNode, fieldInfo.getPointNumBytes(), totMaxSize);
                    BKDConfig config = new BKDConfig(
                        fieldInfo.getPointDimensionCount(),
                        fieldInfo.getPointIndexDimensionCount(),
                        fieldInfo.getPointNumBytes(),
                        effectiveMaxPoints
                    );

                    try (
                        BKDWriter writer = new BKDWriter(
                            writeState.segmentInfo.maxDoc(),
                            writeState.directory,
                            writeState.segmentInfo.name,
                            config,
                            maxMBSortInHeap,
                            totMaxSize
                        )
                    ) {
                        List<PointValues> pointValues = new ArrayList<>();
                        List<MergeState.DocMap> docMaps = new ArrayList<>();
                        for (int i = 0; i < mergeState.pointsReaders.length; i++) {
                            PointsReader reader = mergeState.pointsReaders[i];
                            if (reader != null) {
                                assert reader instanceof Lucene90PointsReader;
                                Lucene90PointsReader reader90 = (Lucene90PointsReader) reader;

                                FieldInfos readerFieldInfos = mergeState.fieldInfos[i];
                                FieldInfo readerFieldInfo = readerFieldInfos.fieldInfo(fieldInfo.name);
                                if (readerFieldInfo != null && readerFieldInfo.getPointDimensionCount() > 0) {
                                    PointValues aPointValues = reader90.getValues(readerFieldInfo.name);
                                    if (aPointValues != null) {
                                        pointValues.add(aPointValues);
                                        docMaps.add(mergeState.docMaps[i]);
                                    }
                                }
                            }
                        }

                        IORunnable finalizer = writer.merge(metaOut, indexOut, dataOut, docMaps, pointValues);
                        if (finalizer != null) {
                            metaOut.writeInt(fieldInfo.number);
                            finalizer.run();
                        }
                    }
                } else {
                    mergeOneField(mergeState, fieldInfo);
                }
            }
        }

        finish();
    }

    @Override
    public void finish() throws IOException {
        if (finished) {
            throw new IllegalStateException("already finished");
        }
        finished = true;
        metaOut.writeInt(-1);
        CodecUtil.writeFooter(indexOut);
        CodecUtil.writeFooter(dataOut);
        metaOut.writeLong(indexOut.getFilePointer());
        metaOut.writeLong(dataOut.getFilePointer());
        CodecUtil.writeFooter(metaOut);
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeWhileHandlingException(metaOut, indexOut, dataOut);
    }

    /**
     * Leaf sizes are always rounded up to a multiple of this value to take advantage of byte alignment.
     */
    static final int LEAF_SIZE_ALIGNMENT = 512;

    static int adjustMaxPointsInLeafNode(int maxPointsInLeafNode, int bytesPerDim, long pointCount) {
        int estimatedBytesPerLeaf = ESTIMATED_OVERHEAD_PER_LEAF_EXCLUDING_SPLIT_VALUES + bytesPerDim;
        long maxLeaves = TARGET_MAX_BKD_HEAP_BYTES / estimatedBytesPerLeaf;
        long desiredLeafSize = (pointCount + maxLeaves - 1) / maxLeaves;
        int capped = (int) Math.min(desiredLeafSize, MAX_POINTS_IN_LEAF_NODE_UPPER_BOUND);
        int result = Math.max(maxPointsInLeafNode, capped);
        return ((result + LEAF_SIZE_ALIGNMENT - 1) / LEAF_SIZE_ALIGNMENT) * LEAF_SIZE_ALIGNMENT;
    }

    static PointsReader fieldsReader(SegmentReadState state) throws IOException {
        return new Lucene90PointsReader(state);
    }
}
