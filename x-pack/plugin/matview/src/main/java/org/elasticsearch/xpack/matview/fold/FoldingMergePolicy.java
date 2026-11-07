/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.matview.fold;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.OneMergeWrappingMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.InfoStream;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Folds documents that share a group key into one document when segments merge.
 * <p>
 * Lucene merges map every live input document to one output document. This policy keeps that contract but shrinks
 * the input: for every group key seen in a merge, one document is the representative and every other document with
 * the same key is hidden through the reader's live docs. Postings, stored fields, points and vectors of hidden
 * documents then disappear through the ordinary with-deletions merge path, and the reduced fields of the
 * representative are served with the group's combined values, so the merged segment ends up with one document per
 * group. How groups are found and reduced is the {@link FoldStrategy}'s business.
 * <p>
 * Requires a reader whose live docs may be narrowed by a merge wrapper after every reader of the merge has been
 * wrapped; with soft deletes enabled this needs a Lucene change in {@code IndexWriter#mergeMiddle}.
 */
public final class FoldingMergePolicy extends OneMergeWrappingMergePolicy {

    private final MergePolicy in;

    public FoldingMergePolicy(MergePolicy in, FoldSpec spec, FoldStrategy strategy, @Nullable FoldGuard guard) {
        super(in, toWrap -> new FoldingOneMerge(toWrap, strategy.newMergeFold(spec, guard)));
        this.in = in;
    }

    /**
     * A lone segment without deletes counts as already merged, so the wrapped policy leaves it alone and the
     * contributions it holds would never fold. An explicit merge down to one segment is the caller's way of asking
     * for a fold, so in that case the wrapped policy is asked again with the segment reported as having deletes to
     * reclaim: it then rewrites the segment on its own, with every nested policy applied as for any other merge.
     * The output is registered as a cascaded segment, which is not an original, so the forced merge terminates.
     */
    @Override
    public MergeSpecification findForcedMerges(
        SegmentInfos segmentInfos,
        int maxSegmentCount,
        Map<SegmentCommitInfo, Boolean> segmentsToMerge,
        MergeContext mergeContext
    ) throws IOException {
        MergeSpecification merges = super.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, mergeContext);
        if (merges != null || maxSegmentCount != 1 || segmentInfos.size() != 1) {
            return merges;
        }
        SegmentCommitInfo only = segmentInfos.info(0);
        if (Boolean.TRUE.equals(segmentsToMerge.get(only)) == false
            || mergeContext.getMergingSegments().contains(only)
            || only.info.maxDoc() < 2) {
            return null;
        }
        return super.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, new MergeContext() {
            @Override
            public int numDeletesToMerge(SegmentCommitInfo info) throws IOException {
                int real = mergeContext.numDeletesToMerge(info);
                return info == only ? Math.max(1, real) : real;
            }

            @Override
            public int numDeletedDocs(SegmentCommitInfo info) {
                return mergeContext.numDeletedDocs(info);
            }

            @Override
            public InfoStream getInfoStream() {
                return mergeContext.getInfoStream();
            }

            @Override
            public Set<SegmentCommitInfo> getMergingSegments() {
                return mergeContext.getMergingSegments();
            }
        });
    }

    /** One instance per merge; carries the per-merge state because readers are wrapped one at a time. */
    static final class FoldingOneMerge extends MergePolicy.OneMerge {
        private final MergePolicy.OneMerge toWrap;
        private final FoldStrategy.MergeFold fold;

        FoldingOneMerge(MergePolicy.OneMerge toWrap, FoldStrategy.MergeFold fold) {
            super(toWrap.segments);
            this.toWrap = toWrap;
            this.fold = fold;
        }

        @Override
        public CodecReader wrapForMerge(CodecReader reader) throws IOException {
            return fold.wrap(toWrap.wrapForMerge(reader));
        }

        @Override
        public void mergeFinished(boolean success, boolean segmentDropped) throws IOException {
            try {
                toWrap.mergeFinished(success, segmentDropped);
            } finally {
                fold.close();
            }
        }
    }
}
