/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link FilterMergePolicy} that interleaves eldest and newest segments picked by {@link MergePolicy#findForcedMerges}
 * and {@link MergePolicy#findForcedDeletesMerges}. This allows time-based indices, that usually have the eldest documents
 * first, to be efficient at finding the most recent documents too.
 */
public class ShuffleForcedMergePolicy extends FilterMergePolicy {
    private static final String SHUFFLE_MERGE_KEY = "es.shuffle_merge";

    public ShuffleForcedMergePolicy(MergePolicy in) {
        super(in);
    }

    /**
     * Return <code>true</code> if the provided reader was merged with interleaved segments.
     */
    public static boolean isInterleavedSegment(LeafReader reader) {
        SegmentReader segReader = Lucene.segmentReader(reader);
        return segReader.getSegmentInfo().info.getDiagnostics().containsKey(SHUFFLE_MERGE_KEY);
    }


    @Override
    public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
        return wrap(in.findForcedDeletesMerges(segmentInfos, mergeContext));
    }

    @Override
    public MergeSpecification findForcedMerges(SegmentInfos segmentInfos, int maxSegmentCount,
                                               Map<SegmentCommitInfo, Boolean> segmentsToMerge,
                                               MergeContext mergeContext) throws IOException {
        return wrap(in.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, mergeContext));
    }

    /** A wrapping one merge that allows reordering of merge infos, and adds shuffle merge diagnostics. */
    static class WrappedOneMerge extends OneMerge {
        // Additional diagnostics Map, containing a single shuffle merge entry.
        private static final Map<String,String> SHUFFLE_MERGE_DIAGNOSTIC = Map.of(SHUFFLE_MERGE_KEY, "");
        private final OneMerge original;

        WrappedOneMerge(OneMerge original, List<SegmentCommitInfo> mergeInfos) {
            super(mergeInfos);
            this.original = original;
        }
        @Override
        public CodecReader wrapForMerge(CodecReader reader) throws IOException {
            return original.wrapForMerge(reader);
        }

        @Override
        public void setMergeInfo(SegmentCommitInfo info) {
            // record that this segment was merged with interleaved segments
            SegmentCommitInfo newInfo = copySegmentCommitInfo(info, SHUFFLE_MERGE_DIAGNOSTIC);
            super.setMergeInfo(newInfo);
        }
    }

    private MergeSpecification wrap(MergeSpecification mergeSpec) throws IOException {
        if (mergeSpec == null) {
            return null;
        }
        MergeSpecification newMergeSpec = new MergeSpecification();
        for (OneMerge toWrap : mergeSpec.merges) {
            List<SegmentCommitInfo> newInfos = interleaveList(new ArrayList<>(toWrap.segments));
            newMergeSpec.add(new WrappedOneMerge(toWrap, newInfos));
        }

        return newMergeSpec;
    }

    // Return a new list that sort segments of the original one by name (older first)
    // and then interleave them to colocate oldest and most recent segments together.
    private List<SegmentCommitInfo> interleaveList(List<SegmentCommitInfo> infos) throws IOException {
        List<SegmentCommitInfo> newInfos = new ArrayList<>(infos.size());
        Collections.sort(infos, Comparator.comparing(a -> a.info.name));
        int left = 0;
        int right = infos.size() - 1;
        while (left <= right) {
            SegmentCommitInfo leftInfo = infos.get(left);
            if (left == right) {
                newInfos.add(infos.get(left));
            } else {
                SegmentCommitInfo rightInfo = infos.get(right);
                // smaller segment first
                if (leftInfo.sizeInBytes() < rightInfo.sizeInBytes()) {
                    newInfos.add(leftInfo);
                    newInfos.add(rightInfo);
                } else {
                    newInfos.add(rightInfo);
                    newInfos.add(leftInfo);
                }
            }
            left ++;
            right --;
        }
        return newInfos;
    }

    /** Returns the maxDoc from the given segmentInfo, or the default value -1. */
    private static int maxDocOrDefault(SegmentInfo segmentInfo) {
        try {
            return segmentInfo.maxDoc();
        } catch (IllegalStateException e) {
            return -1; // default not-yet-set value
        }
    }

    /** Copies the segment commit info and adds the given additional diagnostics. */
    static SegmentCommitInfo copySegmentCommitInfo(SegmentCommitInfo info,
                                                   Map<String,String> additionDiagnostics) {
        Map<String, String> newDiagnostics = new HashMap<>(info.info.getDiagnostics());
        newDiagnostics.putAll(additionDiagnostics);

        SegmentInfo oldSegmentInfo = info.info;
        // Same SI as before but with additional diagnostics
        SegmentInfo newSegmentInfo =
            new SegmentInfo(
                oldSegmentInfo.dir,
                oldSegmentInfo.getVersion(),
                oldSegmentInfo.getMinVersion(),
                oldSegmentInfo.name,
                maxDocOrDefault(oldSegmentInfo),
                oldSegmentInfo.getUseCompoundFile(),
                oldSegmentInfo.getCodec(),
                newDiagnostics,
                oldSegmentInfo.getId(),
                oldSegmentInfo.getAttributes(),
                oldSegmentInfo.getIndexSort());
        SegmentCommitInfo newSegmentCommitInfo =
            new SegmentCommitInfo(
                newSegmentInfo,
                info.getDelCount(),
                info.getSoftDelCount(),
                info.getDelGen(),
                info.getFieldInfosGen(),
                info.getDocValuesGen(),
                info.getId());
        return newSegmentCommitInfo;
    }
}
