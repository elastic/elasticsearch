/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.facet;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.lucene.docset.DocSet;
import org.elasticsearch.common.lucene.docset.DocSets;
import org.elasticsearch.common.lucene.search.AndFilter;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public abstract class AbstractFacetCollector extends FacetCollector {

    protected final String facetName;

    protected Filter filter;

    private DocSet docSet = null;

    public AbstractFacetCollector(String facetName) {
        this.facetName = facetName;
    }

    public Filter getFilter() {
        return this.filter;
    }

    @Override public void setFilter(Filter filter) {
        if (this.filter == null) {
            this.filter = filter;
        } else {
            this.filter = new AndFilter(ImmutableList.of(filter, this.filter));
        }
    }

    @Override public void setScorer(Scorer scorer) throws IOException {
        // usually, there is nothing to do here
    }

    @Override public boolean acceptsDocsOutOfOrder() {
        return true; // when working on FieldData, docs can be out of order
    }

    @Override public void setNextReader(IndexReader reader, int docBase) throws IOException {
        if (filter != null) {
            docSet = DocSets.convert(reader, filter.getDocIdSet(reader));
        }
        doSetNextReader(reader, docBase);
    }

    protected abstract void doSetNextReader(IndexReader reader, int docBase) throws IOException;

    @Override public void collect(int doc) throws IOException {
        if (docSet == null) {
            doCollect(doc);
        } else if (docSet.get(doc)) {
            doCollect(doc);
        }
    }

    protected abstract void doCollect(int doc) throws IOException;
}
