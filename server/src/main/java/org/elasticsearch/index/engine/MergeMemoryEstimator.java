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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.VectorEncoding;
import org.elasticsearch.common.lucene.Lucene;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Provides an estimation of the memory needed to merge segments.
 *
 * This class is a temporary solution until we have a better way to estimate the memory needed for merges in Lucene
 * (see the corresponding <a href="https://github.com/apache/lucene/issues/14225">Lucene issue</a>)
 * We can work iteratively in providing estimations for different types of fields and vector encodings.
 */
public class MergeMemoryEstimator {

    // Determined empirically by using Accountable.ramBytesUsed() during merges on Lucene using an instrumented build of Lucene.
    // Didn't adapted the ramBytesUsed() code for this as it depends on graph levels and size for non-zero levels, which are difficult
    // to estimate without actually building the graph.
    public static final long HNSW_PER_DOC_ESTIMATION = 348L;

    /**
     * Estimates the memory, in bytes, needed to merge the segments of the given merge.
     */
    public static long estimateMergeMemory(MergePolicy.OneMerge merge, IndexReader indexReader) {
        assert merge.segments.isEmpty() == false;

        long memoryNeeded = 0;
        Map<String, SegmentCommitInfo> segments = merge.segments.stream().collect(Collectors.toMap(s -> s.info.name, s -> s));
        List<LeafReaderContext> leaves = indexReader.leaves();
        SegmentReader segmentReader = null;
        for (LeafReaderContext leafReaderContext : leaves) {
            segmentReader = Lucene.segmentReader(leafReaderContext.reader());
            String segmentName = segmentReader.getSegmentName();
            SegmentCommitInfo segmentCommitInfo = segments.get(segmentName);
            if (segmentCommitInfo != null) {
                memoryNeeded += estimateMergeMemory(segmentCommitInfo, segmentReader);
                segments.remove(segmentName);
                if (segments.isEmpty()) {
                    break;
                }
            }
        }

        // Estimate segments without readers - the searcher may not have been refreshed yet, so estimate them with the field info from
        // the last segment reader
        if (segmentReader != null) {
            for (SegmentCommitInfo segmentCommitInfo : segments.values()) {
                memoryNeeded += estimateMergeMemory(segmentCommitInfo, segmentReader);
            }
        }

        return memoryNeeded;
    }

    private static long estimateMergeMemory(SegmentCommitInfo segmentCommitInfo, SegmentReader reader) {
        long maxMem = 0;
        for (FieldInfo fieldInfo : reader.getFieldInfos()) {
            maxMem = Math.max(maxMem, estimateFieldMemory(fieldInfo, segmentCommitInfo, reader));
        }
        return maxMem;
    }

    private static long estimateFieldMemory(FieldInfo fieldInfo, SegmentCommitInfo segmentCommitInfo, SegmentReader segmentReader) {

        long maxMem = 0;
        if (fieldInfo.hasVectorValues()) {
            maxMem = Math.max(maxMem, estimateVectorFieldMemory(fieldInfo, segmentCommitInfo, segmentReader));
        }
        // TODO Work on estimations on other field infos when / if needed

        return maxMem;
    }

    private static long estimateVectorFieldMemory(FieldInfo fieldInfo, SegmentCommitInfo segmentCommitInfo, SegmentReader segmentReader) {
        KnnVectorsReader vectorsReader = segmentReader.getVectorReader();
        if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader perFieldKnnVectorsFormat) {
            vectorsReader = perFieldKnnVectorsFormat.getFieldReader(fieldInfo.getName());
        }

        return getVectorFieldEstimation(fieldInfo, segmentCommitInfo, vectorsReader);
    }

    private static long getVectorFieldEstimation(FieldInfo fieldInfo, SegmentCommitInfo segmentCommitInfo, KnnVectorsReader vectorsReader) {
        int numDocs = segmentCommitInfo.info.maxDoc() - segmentCommitInfo.getDelCount();
        if (vectorsReader instanceof Lucene99HnswVectorsReader) {
            return numDocs * HNSW_PER_DOC_ESTIMATION;

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
