/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.search.facet.terms.strings;

import com.google.common.collect.ImmutableSet;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 *
 */
public class TermsStringFacetExecutor extends FacetExecutor {

    private final IndexFieldData indexFieldData;
    private final TermsFacet.ComparatorType comparatorType;
    private final SearchScript script;

    private final int shardSize;
    private final int size;

    // the aggregation map
    long missing;
    long total;
    private final boolean allTerms;
    private final HashedAggregator aggregator;

    public TermsStringFacetExecutor(IndexFieldData indexFieldData, int size, int shardSize, TermsFacet.ComparatorType comparatorType, boolean allTerms, SearchContext context,
                                    ImmutableSet<BytesRef> excluded, Pattern pattern, SearchScript script) {
        this.indexFieldData = indexFieldData;
        this.size = size;
        this.shardSize = shardSize;
        this.comparatorType = comparatorType;
        this.script = script;
        this.allTerms = allTerms;

        if (excluded.isEmpty() && pattern == null && script == null) {
            aggregator = new HashedAggregator();
        } else {
            aggregator = new HashedScriptAggregator(excluded, pattern, script);
        }

        if (allTerms) {
            loadAllTerms(context, indexFieldData, aggregator);
        }
    }

    @Override
    public Collector collector() {
        return new Collector(aggregator, allTerms);
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        try {
            return HashedAggregator.buildFacet(facetName, size, shardSize, missing, total, comparatorType, aggregator);
        } finally {
            aggregator.release();
        }
    }

    final class Collector extends FacetExecutor.Collector {

        private final HashedAggregator aggregator;
        private final boolean allTerms;
        private BytesValues values;

        Collector(HashedAggregator aggregator, boolean allTerms) {
            this.aggregator = aggregator;
            this.allTerms = allTerms;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            if (script != null) {
                script.setScorer(scorer);
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            values = indexFieldData.load(context).getBytesValues(true);
            if (script != null) {
                script.setNextReader(context);
            }
        }

        @Override
        public void collect(int doc) throws IOException {
            aggregator.onDoc(doc, values);
        }

        @Override
        public void postCollection() {
            TermsStringFacetExecutor.this.missing = aggregator.missing();
            TermsStringFacetExecutor.this.total = aggregator.total();
        }
    }

    static void loadAllTerms(SearchContext context, IndexFieldData indexFieldData, HashedAggregator aggregator) {

        for (AtomicReaderContext readerContext : context.searcher().getTopReaderContext().leaves()) {
            int maxDoc = readerContext.reader().maxDoc();
            if (indexFieldData instanceof IndexFieldData.WithOrdinals) {
                BytesValues.WithOrdinals values = ((IndexFieldData.WithOrdinals) indexFieldData).load(readerContext).getBytesValues(false);
                Ordinals.Docs ordinals = values.ordinals();
                // 0 = docs with no value for field, so start from 1 instead
                for (long ord = Ordinals.MIN_ORDINAL; ord < ordinals.getMaxOrd(); ord++) {
                    BytesRef value = values.getValueByOrd(ord);
                    aggregator.addValue(value, value.hashCode(), values);
                }
            } else {
                BytesValues values = indexFieldData.load(readerContext).getBytesValues(true);
                for (int docId = 0; docId < maxDoc; docId++) {
                    final int size = values.setDocument(docId);
                    for (int i = 0; i < size; i++) {
                        final BytesRef value = values.nextValue();
                        aggregator.addValue(value, values.currentValueHash(), values);
                    }
                }
            }
        }
    }

}
