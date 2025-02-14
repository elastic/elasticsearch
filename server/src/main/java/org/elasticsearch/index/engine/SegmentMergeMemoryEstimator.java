/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.VectorEncoding;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.List;

public class SegmentMergeMemoryEstimator {

    public static long estimateSegmentMemory(MergePolicy.OneMerge merge, IndexReader indexReader) {
        long memoryNeeded = 0;
        for (SegmentCommitInfo mergedSegment : merge.segments) {
            memoryNeeded += estimateSegmentMemory(mergedSegment.info.name, indexReader);
        }
        return memoryNeeded;
    }

    private static long estimateSegmentMemory(String segmentName, IndexReader indexReader) {
        List<LeafReaderContext> leaves = indexReader.leaves();
        for (LeafReaderContext leafReaderContext : leaves) {
            SegmentReader segmentReader = Lucene.segmentReader(leafReaderContext.reader());
            if (segmentReader.getSegmentName().equals(segmentName)) {
                return estimateSegmentMemory(segmentReader);
            }
        }

        // Segment not found, we can't estimate it
        return 0;
    }

    private static long estimateSegmentMemory(SegmentReader reader) {
        long maxMem = 0;
        for (FieldInfo fieldInfo : reader.getFieldInfos()) {
            maxMem = Math.max(maxMem, estimateFieldMemory(fieldInfo, reader).getBytes());
        }
        return maxMem;
    }

    private static ByteSizeValue estimateFieldMemory(FieldInfo fieldInfo, SegmentReader segmentReader) {

        long maxMem = 0;
        if (fieldInfo.hasVectorValues()) {
            maxMem = Math.max(maxMem, estimateVectorFieldMemory(fieldInfo, segmentReader));
        }
        // TODO Work on estimations on other field infos when / if needed

        return ByteSizeValue.ofBytes(maxMem);
    }

    private static long estimateVectorFieldMemory(FieldInfo fieldInfo, SegmentReader segmentReader) {
        long maxMem = 0;
        for (LeafReaderContext ctx : segmentReader.leaves()) {
            CodecReader codecReader = (CodecReader) FilterLeafReader.unwrap(ctx.reader());
            KnnVectorsReader vectorsReader = codecReader.getVectorReader();
            if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader perFieldKnnVectorsFormat) {
                vectorsReader = perFieldKnnVectorsFormat.getFieldReader(fieldInfo.getName());
            }

            final long estimation = getVectorFieldEstimation(fieldInfo, segmentReader, vectorsReader);
            maxMem = Math.max(maxMem, estimation);
        }
        return maxMem;
    }

    private static long getVectorFieldEstimation(FieldInfo fieldInfo, SegmentReader segmentReader, KnnVectorsReader vectorsReader) {
        int numDocs = segmentReader.numDocs();
        if (vectorsReader instanceof Lucene99HnswVectorsReader) {
            // Determined empirically from graph usage on merges, as it's complicated to estimate graph levels and size for non-zero levels
            return numDocs * 348L;

        } else {
            // Dominated by the heap byte buffer size used to write each vector
            if (fieldInfo.getVectorEncoding() == VectorEncoding.FLOAT32) {
                return fieldInfo.getVectorDimension() * VectorEncoding.FLOAT32.byteSize;
            }
            // Byte does not use buffering for writing but the IndexOutput directly
            return 0;
        }
    }

}
