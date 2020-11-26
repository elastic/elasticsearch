/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
