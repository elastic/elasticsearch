/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.MergeScheduler;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.OnGoingMerge;

import java.util.Set;

public interface ElasticsearchMergeScheduler {

    Set<OnGoingMerge> onGoingMerges();

    MergeStats stats();

    void refreshConfig();

    MergeScheduler getMergeScheduler();

    /**
     * Marks all queued and running merges as aborted before abort-merge-reads is signalled.
     * Implementations backed by {@link ThreadPoolMergeScheduler} call
     * {@link org.apache.lucene.index.MergePolicy.OneMerge#setAborted()} on every pending merge so
     * that {@code merge.isAborted()} is {@code true} before any
     * {@link org.apache.lucene.index.MergePolicy.MergeAbortedException} can be thrown during
     * compound-file creation, preventing a corrupt-segment write in Lucene's {@code mergeMiddle()}.
     * Other implementations may provide a no-op default.
     */
    default void abortAllMerges() {}
}
