/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.elasticsearch.search.dfs.CachedDfSource;
import org.elasticsearch.util.collect.Lists;
import org.elasticsearch.util.lucene.MultiCollector;

import java.io.IOException;
import java.util.List;

/**
 * @author kimchy (shay.banon)
 */
public class ContextIndexSearcher extends IndexSearcher {

    private SearchContext searchContext;

    private CachedDfSource dfSource;

    private List<Collector> collectors;

    private List<Collector> globalCollectors;

    private boolean useGlobalCollectors = false;

    public ContextIndexSearcher(SearchContext searchContext, IndexReader r) {
        super(r);
        this.searchContext = searchContext;
    }

    public IndexReader[] subReaders() {
        return this.subReaders;
    }

    public int[] docStarts() {
        return this.docStarts;
    }

    // taken from DirectoryReader#readerIndex

    public int readerIndex(int doc) {
        int lo = 0;                                      // search starts array
        int hi = subReaders.length - 1;                  // for first element less

        while (hi >= lo) {
            int mid = (lo + hi) >>> 1;
            int midValue = docStarts[mid];
            if (doc < midValue)
                hi = mid - 1;
            else if (doc > midValue)
                lo = mid + 1;
            else {                                      // found a match
                while (mid + 1 < subReaders.length && docStarts[mid + 1] == midValue) {
                    mid++;                                  // scan to last match
                }
                return mid;
            }
        }
        return hi;
    }

    public void dfSource(CachedDfSource dfSource) {
        this.dfSource = dfSource;
    }

    public void addCollector(Collector collector) {
        if (collectors == null) {
            collectors = Lists.newArrayList();
        }
        collectors.add(collector);
    }

    public List<Collector> collectors() {
        return collectors;
    }

    public void addGlobalCollector(Collector collector) {
        if (globalCollectors == null) {
            globalCollectors = Lists.newArrayList();
        }
        globalCollectors.add(collector);
    }

    public List<Collector> globalCollectors() {
        return globalCollectors;
    }

    public void useGlobalCollectors(boolean useGlobalCollectors) {
        this.useGlobalCollectors = useGlobalCollectors;
    }

    @Override public Query rewrite(Query original) throws IOException {
        if (original == searchContext.query() || original == searchContext.originalQuery()) {
            // optimize in case its the top level search query and we already rewrote it...
            if (searchContext.queryRewritten()) {
                return searchContext.query();
            }
            Query rewriteQuery = super.rewrite(original);
            searchContext.updateRewriteQuery(rewriteQuery);
            return rewriteQuery;
        } else {
            return super.rewrite(original);
        }
    }

    @Override protected Weight createWeight(Query query) throws IOException {
        if (dfSource == null) {
            return super.createWeight(query);
        }
        return query.weight(dfSource);
    }

    @Override public void search(Weight weight, Filter filter, Collector collector) throws IOException {
        if (searchContext.timeout() != null) {
            collector = new TimeLimitingCollector(collector, searchContext.timeout().millis());
        }
        if (useGlobalCollectors) {
            if (globalCollectors != null) {
                collector = new MultiCollector(collector, globalCollectors.toArray(new Collector[globalCollectors.size()]));
            }
        } else {
            if (collectors != null) {
                collector = new MultiCollector(collector, collectors.toArray(new Collector[collectors.size()]));
            }
        }
        // we only compute the doc id set once since within a context, we execute the same query always...
        if (searchContext.timeout() != null) {
            searchContext.queryResult().searchTimedOut(false);
            try {
                super.search(weight, filter, collector);
            } catch (TimeLimitingCollector.TimeExceededException e) {
                searchContext.queryResult().searchTimedOut(true);
            }
        } else {
            super.search(weight, filter, collector);
        }
    }
}