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

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.terms.strings.HashedAggregator.BytesRefCountIterator;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;
import org.elasticsearch.search.internal.SearchContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 *
 */
public class FieldsTermsStringFacetExecutor extends FacetExecutor {

    private final InternalStringTermsFacet.ComparatorType comparatorType;
    private final int size;
    private final IndexFieldData[] indexFieldDatas;
    private final SearchScript script;
    private final HashedAggregator aggregator;
    long missing;
    long total;


    public FieldsTermsStringFacetExecutor(String facetName, String[] fieldsNames, int size, InternalStringTermsFacet.ComparatorType comparatorType, boolean allTerms, SearchContext context,
                                          ImmutableSet<BytesRef> excluded, Pattern pattern, SearchScript script) {
        this.size = size;
        this.comparatorType = comparatorType;
        this.script = script;
        this.indexFieldDatas = new IndexFieldData[fieldsNames.length];
        for (int i = 0; i < fieldsNames.length; i++) {
            FieldMapper mapper = context.smartNameFieldMapper(fieldsNames[i]);
            if (mapper == null) {
                throw new FacetPhaseExecutionException(facetName, "failed to find mapping for [" + fieldsNames[i] + "]");
            }
            indexFieldDatas[i] = context.fieldData().getForField(mapper);
        }
        if (excluded.isEmpty() && pattern == null && script == null) {
            aggregator = new HashedAggregator();
        } else {
            aggregator = new HashedScriptAggregator(excluded, pattern, script);
        }

        // TODO: we need to support this flag with the new field data...
//        if (allTerms) {
//            try {
//                for (int i = 0; i < fieldsNames.length; i++) {
//                    for (AtomicReaderContext readerContext : context.searcher().getTopReaderContext().leaves()) {
//                        FieldData fieldData = fieldDataCache.cache(fieldsDataType[i], readerContext.reader(), indexFieldsNames[i]);
//                        fieldData.forEachValue(aggregator);
//                    }
//                }
//            } catch (Exception e) {
//                throw new FacetPhaseExecutionException(facetName, "failed to load all terms", e);
//            }
//        }
    }

    @Override
    public Collector collector() {
        return new Collector(aggregator);
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        return HashedAggregator.buildFacet(facetName, size, missing, total, comparatorType, aggregator);
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
                values[i] = indexFieldDatas[i].load(context).getBytesValues();
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
