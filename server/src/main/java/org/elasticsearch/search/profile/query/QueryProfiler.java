/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.search.profile.AbstractProfiler;

import java.util.Objects;

/**
 * This class acts as a thread-local storage for profiling a query.  It also
 * builds a representation of the query tree which is built constructed
 * "online" as the weights are wrapped by ContextIndexSearcher.  This allows us
 * to know the relationship between nodes in tree without explicitly
 * walking the tree or pre-wrapping everything
 *
 * A Profiler is associated with every Search, not per Search-Request. E.g. a
 * request may execute two searches (query + global agg).  A Profiler just
 * represents one of those
 */
public final class QueryProfiler extends AbstractProfiler<QueryProfileBreakdown, Query> {

    /**
     * The root Collector used in the search
     */
    private MultiProfileCollectorManager collectorManager;

    public QueryProfiler() {
        super(new InternalQueryProfileTree());
    }

    /** Set the collector manager that is associated with this profiler. */
    public void setCollectorManager(MultiProfileCollectorManager collectorManager) {
        if (this.collectorManager != null) {
            throw new IllegalStateException("The collector manager can only be set once.");
        }
        this.collectorManager = Objects.requireNonNull(collectorManager);
    }

    /**
     * Begin timing the rewrite phase of a request.  All rewrites are accumulated together into a
     * single metric
     */
    public void startRewriteTime() {
        ((InternalQueryProfileTree) profileTree).startRewriteTime();
    }

    /**
     * Stop recording the current rewrite and add it's time to the total tally, returning the
     * cumulative time so far.
     *
     * @return cumulative rewrite time
     */
    public long stopAndAddRewriteTime() {
        return ((InternalQueryProfileTree) profileTree).stopAndAddRewriteTime();
    }

    /**
     * @return total time taken to rewrite all queries in this profile
     */
    public long getRewriteTime() {
        return ((InternalQueryProfileTree) profileTree).getRewriteTime();
    }

    /**
     * Return the current root Collector for this search
     */
    public CollectorResult getCollector() {
        return collectorManager.getCollectorTree();
    }

}
