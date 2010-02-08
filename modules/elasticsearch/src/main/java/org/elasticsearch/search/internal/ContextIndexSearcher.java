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
import org.apache.lucene.util.OpenBitSet;
import org.elasticsearch.search.dfs.CachedDfSource;
import org.elasticsearch.util.lucene.docidset.DocIdSetCollector;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class ContextIndexSearcher extends IndexSearcher {

    private SearchContext searchContext;

    private CachedDfSource dfSource;

    private boolean docIdSetEnabled;

    private OpenBitSet docIdSet;

    public ContextIndexSearcher(SearchContext searchContext, IndexReader r) {
        super(r);
        this.searchContext = searchContext;
    }

    public void dfSource(CachedDfSource dfSource) {
        this.dfSource = dfSource;
    }

    public void enabledDocIdSet() {
        docIdSetEnabled = true;
    }

    public OpenBitSet docIdSet() {
        return docIdSet;
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
        // we only compute the doc id set once since within a context, we execute the same query always...
        if (docIdSetEnabled && docIdSet == null) {
            collector = new DocIdSetCollector(collector, getIndexReader());
        }
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
        if (docIdSetEnabled && docIdSet == null) {
            this.docIdSet = ((DocIdSetCollector) collector).docIdSet();
        }
    }
}