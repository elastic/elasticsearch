/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.Objects;

public final class LocalLogicalOptimizerContext extends LogicalOptimizerContext {
    private final SearchStats searchStats;

    public LocalLogicalOptimizerContext(Configuration configuration, FoldContext foldCtx, SearchStats searchStats) {
        super(configuration, foldCtx);
        this.searchStats = searchStats;
    }

    public SearchStats searchStats() {
        return searchStats;
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
