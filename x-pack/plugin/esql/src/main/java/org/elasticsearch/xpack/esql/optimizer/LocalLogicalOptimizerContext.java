/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.Objects;

public final class LocalLogicalOptimizerContext extends LogicalOptimizerContext {
    private final SearchStats searchStats;

    public LocalLogicalOptimizerContext(Configuration configuration, FoldContext foldCtx, SearchStats searchStats) {
        super(configuration, foldCtx, null);
        this.searchStats = searchStats;
    }

    public SearchStats searchStats() {
        return searchStats;
    }

    /**
     * The minimum transport version is not sent to data nodes, so this is currently unsupported.
     * <p>
     * This can be changed in the future if e.g. lookup joins need to become aware of the minimum transport version.
     * (Lookup joins are remote, and for now we have to plan as if the remote node was on the oldest compatible version, which is limiting.)
     */
    @Override
    public TransportVersion minimumVersion() {
        throw new UnsupportedOperationException("Minimum version is not sent to data nodes");
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            var that = (LocalLogicalOptimizerContext) obj;
            return Objects.equals(this.searchStats, that.searchStats);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), searchStats);
    }

    @Override
    public String toString() {
        return "LocalLogicalOptimizerContext[" + "configuration=" + configuration() + ", " + "searchStats=" + searchStats + ']';
    }
}
