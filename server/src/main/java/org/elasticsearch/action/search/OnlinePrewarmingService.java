/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.index.shard.IndexShard;

/**
 * Interface for prewarming the segments of a shard, tailored for consumption at
 * higher volumes than alternative warming strategies (i.e. offline / recovery warming)
 * that are more speculative.
 */
public interface OnlinePrewarmingService {
    OnlinePrewarmingService NOOP = indexShard -> {};

    /**
     * Prewarms resources (typically segments) for the given index shard.
     *
     * @param indexShard the index shard for which resources should be prewarmed
     */
    void prewarm(IndexShard indexShard);
}
