/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

/**
 * SPI for computing the warming ratio for a compound commit.
 * Implementation is to be provided via {@link WarmingRatioProviderFactory}.
 */
public interface WarmingRatioProvider {
    /**
     * Computes the warming ratio for a compound commit, determining what fraction of its data
     * should be pre-warmed into the shared blob cache. Currently, only used on search nodes.
     * A ratio of 0 means no pre-warming; 1 means full pre-warming.
     */
    double getWarmingRatio(ObjectStoreService.StatelessCompoundCommitReferenceWithInternalFiles referencedCompoundCommit, long nowMillis);
}
