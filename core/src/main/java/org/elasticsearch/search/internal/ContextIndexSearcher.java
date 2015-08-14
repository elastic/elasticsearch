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

package org.elasticsearch.search.internal;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.search.dfs.CachedDfSource;
import org.elasticsearch.search.internal.SearchContext.Lifetime;

import java.io.IOException;
import java.util.List;

/**
 * Context-aware extension of {@link IndexSearcher}.
 */
public class ContextIndexSearcher extends IndexSearcher implements Releasable {

    /** The wrapped {@link IndexSearcher}. The reason why we sometimes prefer delegating to this searcher instead of <tt>super</tt> is that
     *  this instance may have more assertions, for example if it comes from MockInternalEngine which wraps the IndexSearcher into an
     *  AssertingIndexSearcher. */
    private final IndexSearcher in;

    private final SearchContext searchContext;

    private CachedDfSource dfSource;

    public ContextIndexSearcher(SearchContext searchContext, Engine.Searcher searcher) {
        super(searcher.reader());
        in = searcher.searcher();
        this.searchContext = searchContext;
        setSimilarity(searcher.searcher().getSimilarity(true));
    }

    @Override
    public void close() {
    }

    public void dfSource(CachedDfSource dfSource) {
        this.dfSource = dfSource;
    }

    @Override
    public Query rewrite(Query original) throws IOException {
        try {
            return in.rewrite(original);
        } catch (Throwable t) {
            searchContext.clearReleasables(Lifetime.COLLECTION);
            throw ExceptionsHelper.convertToElastic(t);
        }
    }

    @Override
    public Weight createNormalizedWeight(Query query, boolean needsScores) throws IOException {
        try {
            // if scores are needed and we have dfs data then use it
            if (dfSource != null && needsScores) {
                return dfSource.createNormalizedWeight(query, needsScores);
            }
            return in.createNormalizedWeight(query, needsScores);
        } catch (Throwable t) {
            searchContext.clearReleasables(Lifetime.COLLECTION);
            throw ExceptionsHelper.convertToElastic(t);
        }
    }

    @Override
    public Explanation explain(Query query, int doc) throws IOException {
        try {
            return in.explain(query, doc);
        } finally {
            searchContext.clearReleasables(Lifetime.COLLECTION);
        }
    }

    @Override
    protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
        try {
            super.search(leaves, weight, collector);
        } finally {
            searchContext.clearReleasables(Lifetime.COLLECTION);
        }
    }
}
