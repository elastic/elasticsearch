/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.context;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.rank.RankShardResult;

/**
 * {@link RankFeaturePhaseRankShardContext} is a base class used to execute the RankFeature phase on each shard.
 * In this class, we can fetch the feature data for a given set of documents and pass them back to the coordinator
 * through the {@link RankShardResult}.
 */
public abstract class RankFeaturePhaseRankShardContext {

    protected final String field;

    public RankFeaturePhaseRankShardContext(final String field) {
        this.field = field;
    }

    public String getField() {
        return field;
    }

    /**
     * This is used once we have read the needed field data for a given set of documents on the {@code RankFeatureShardPhase},
     * using the {@link  org.elasticsearch.search.fetch.FetchPhase} and the {@link org.elasticsearch.search.fetch.subphase.FetchFieldsPhase}
     * sub-phase, to store them in a {@code RankFeatureDoc} and passed them back to the coordinator.
     */
    @Nullable
    public abstract RankShardResult buildRankFeatureShardResult();
}
