/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.search.profile.AbstractInternalProfileTree;
import org.elasticsearch.search.profile.ProfileResult;

/**
 * This class tracks the dependency tree for queries (scoring and rewriting) and
 * generates {@link QueryProfileBreakdown} for each node in the tree.  It also finalizes the tree
 * and returns a list of {@link ProfileResult} that can be serialized back to the client
 */
final class InternalQueryProfileTree extends AbstractInternalProfileTree<QueryProfileBreakdown, Query> {

    /** Rewrite time */
    private long rewriteTime;
    private long rewriteScratch;

    @Override
    protected QueryProfileBreakdown createProfileBreakdown() {
        return new QueryProfileBreakdown();
    }

    @Override
    protected String getTypeFromElement(Query query) {
        // Anonymous classes won't have a name,
        // we need to get the super class
        if (query.getClass().getSimpleName().isEmpty()) {
            return query.getClass().getSuperclass().getSimpleName();
        }
        return query.getClass().getSimpleName();
    }

    @Override
    protected String getDescriptionFromElement(Query query) {
        return query.toString();
    }

    /**
     * Begin timing a query for a specific Timing context
     */
    public void startRewriteTime() {
        assert rewriteScratch == 0;
        rewriteScratch = System.nanoTime();
    }

    /**
     * Halt the timing process and add the elapsed rewriting time.
     * startRewriteTime() must be called for a particular context prior to calling
     * stopAndAddRewriteTime(), otherwise the elapsed time will be negative and
     * nonsensical
     *
     * @return          The elapsed time
     */
    public long stopAndAddRewriteTime() {
        long time = Math.max(1, System.nanoTime() - rewriteScratch);
        rewriteTime += time;
        rewriteScratch = 0;
        return time;
    }

    public long getRewriteTime() {
        return rewriteTime;
    }
}
