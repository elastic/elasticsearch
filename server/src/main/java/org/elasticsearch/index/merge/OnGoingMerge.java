/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.merge;

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;

import java.util.List;

/**
 * Represents a single on going merge within an index.
 */
public class OnGoingMerge {

    private final String id;
    private final MergePolicy.OneMerge oneMerge;

    public OnGoingMerge(MergePolicy.OneMerge merge) {
        this.id = Integer.toString(System.identityHashCode(merge));
        this.oneMerge = merge;

    }

    /**
     * A unique id for the merge.
     */
    public String getId() {
        return id;
    }

    /**
     * Returns the total size in bytes of this merge. Note that this does not
     * indicate the size of the merged segment, but the
     * input total size.
     */
    public long getTotalBytesSize() {
        return oneMerge.totalBytesSize();
    }

    /**
     * The list of segments that are being merged.
     */
    public List<SegmentCommitInfo> getMergedSegments() {
        return oneMerge.segments;
    }
}
