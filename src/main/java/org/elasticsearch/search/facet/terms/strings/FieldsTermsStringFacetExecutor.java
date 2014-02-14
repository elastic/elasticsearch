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

package org.elasticsearch.search.facet.terms.strings;

import com.google.common.collect.ImmutableSet;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 *
 */
public class FieldsTermsStringFacetExecutor extends FacetExecutor {

    private final InternalStringTermsFacet.ComparatorType comparatorType;
    private final int size;
    private final int shardSize;
    private final IndexFieldData[] indexFieldDatas;
    private final SearchScript script;
    private final HashedAggregator aggregator;
    long missing;
    long total;

    public FieldsTermsStringFacetExecutor(FieldMapper[] fieldMappers, int size, int shardSize, InternalStringTermsFacet.ComparatorType comparatorType,
                                          boolean allTerms, SearchContext context, ImmutableSet<BytesRef> excluded, Pattern pattern, SearchScript script) {
        this.size = size;
        this.shardSize = shardSize;
        this.comparatorType = comparatorType;
        this.script = script;
        this.indexFieldDatas = new IndexFieldData[fieldMappers.length];
        for (int i = 0; i < fieldMappers.length; i++) {
            FieldMapper mapper = fieldMappers[i];
            indexFieldDatas[i] = context.fieldData().getForField(mapper);
        }
        if (excluded.isEmpty() && pattern == null && script == null) {
            aggregator = new HashedAggregator();
        } else {
            aggregator = new HashedScriptAggregator(excluded, pattern, script);
        }

        if (allTerms) {
            for (int i = 0; i < fieldMappers.length; i++) {
                TermsStringFacetExecutor.loadAllTerms(context, indexFieldDatas[i], aggregator);
            }
        }
    }

    @Override
    public Collector collector() {
        return new Collector(aggregator);
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        try {
            return HashedAggregator.buildFacet(facetName, size, shardSize, missing, total, comparatorType, aggregator);
        } finally {
            aggregator.release();
        }
    }

    class Collector extends FacetExecutor.Collector {

        private final HashedAggregator aggregator;
        private BytesValues[] values;

        public Collector(HashedAggregator aggregator) {
            values = new BytesValues[indexFieldDatas.length];
            this.aggregator = aggregator;

        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            if (script != null) {
                script.setScorer(scorer);
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            for (int i = 0; i < indexFieldDatas.length; i++) {
                values[i] = indexFieldDatas[i].load(context).getBytesValues(true);
            }
            if (script != null) {
                script.setNextReader(context);
            }
        }

        @Override
        public void collect(int doc) throws IOException {
            for (int i = 0; i < values.length; i++) {
                aggregator.onDoc(doc, values[i]);
            }
        }

        @Override
        public void postCollection() {
            FieldsTermsStringFacetExecutor.this.missing = aggregator.missing();
            FieldsTermsStringFacetExecutor.this.total = aggregator.total();
        }
    }

}
