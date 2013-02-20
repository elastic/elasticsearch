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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.index.fielddata.HashedBytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class FieldsTermsStringFacetExecutor extends FacetExecutor {

    private final InternalStringTermsFacet.ComparatorType comparatorType;
    private final int size;
    private final int numberOfShards;
    private final IndexFieldData[] indexFieldDatas;
    private final SearchScript script;
    private final Pattern pattern;
    private final ImmutableSet<BytesRef> excluded;

    TObjectIntHashMap<HashedBytesRef> facets;
    long missing;
    long total;


    public FieldsTermsStringFacetExecutor(String facetName, String[] fieldsNames, int size, InternalStringTermsFacet.ComparatorType comparatorType, boolean allTerms, SearchContext context,
                                          ImmutableSet<BytesRef> excluded, Pattern pattern, SearchScript script) {
        this.size = size;
        this.comparatorType = comparatorType;
        this.numberOfShards = context.numberOfShards();
        this.script = script;
        this.excluded = excluded;
        this.pattern = pattern;

        facets = CacheRecycler.popObjectIntMap();


        this.indexFieldDatas = new IndexFieldData[fieldsNames.length];
        for (int i = 0; i < fieldsNames.length; i++) {
            FieldMapper mapper = context.smartNameFieldMapper(fieldsNames[i]);
            if (mapper == null) {
                throw new FacetPhaseExecutionException(facetName, "failed to find mapping for [" + fieldsNames[i] + "]");
            }
            indexFieldDatas[i] = context.fieldData().getForField(mapper);
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
        return new Collector();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        if (facets.isEmpty()) {
            CacheRecycler.pushObjectIntMap(facets);
            return new InternalStringTermsFacet(facetName, comparatorType, size, ImmutableList.<InternalStringTermsFacet.TermEntry>of(), missing, total);
        } else {
            if (size < EntryPriorityQueue.LIMIT) {
                EntryPriorityQueue ordered = new EntryPriorityQueue(size, comparatorType.comparator());
                for (TObjectIntIterator<HashedBytesRef> it = facets.iterator(); it.hasNext(); ) {
                    it.advance();
                    ordered.insertWithOverflow(new InternalStringTermsFacet.TermEntry(it.key().bytes, it.value()));
                }
                InternalStringTermsFacet.TermEntry[] list = new InternalStringTermsFacet.TermEntry[ordered.size()];
                for (int i = ordered.size() - 1; i >= 0; i--) {
                    list[i] = ((InternalStringTermsFacet.TermEntry) ordered.pop());
                }
                CacheRecycler.pushObjectIntMap(facets);
                return new InternalStringTermsFacet(facetName, comparatorType, size, Arrays.asList(list), missing, total);
            } else {
                BoundedTreeSet<InternalStringTermsFacet.TermEntry> ordered = new BoundedTreeSet<InternalStringTermsFacet.TermEntry>(comparatorType.comparator(), size);
                for (TObjectIntIterator<HashedBytesRef> it = facets.iterator(); it.hasNext(); ) {
                    it.advance();
                    ordered.add(new InternalStringTermsFacet.TermEntry(it.key().bytes, it.value()));
                }
                CacheRecycler.pushObjectIntMap(facets);
                return new InternalStringTermsFacet(facetName, comparatorType, size, ordered, missing, total);
            }
        }
    }

    class Collector extends FacetExecutor.Collector {

        private final StaticAggregatorValueProc[] aggregators;
        private HashedBytesValues[] values;

        public Collector() {
            values = new HashedBytesValues[indexFieldDatas.length];
            aggregators = new StaticAggregatorValueProc[indexFieldDatas.length];
            for (int i = 0; i < indexFieldDatas.length; i++) {
                if (excluded.isEmpty() && pattern == null && script == null) {
                    aggregators[i] = new StaticAggregatorValueProc(facets);
                } else {
                    aggregators[i] = new AggregatorValueProc(facets, excluded, pattern, script);
                }
            }
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
                values[i] = indexFieldDatas[i].load(context).getHashedBytesValues();
                aggregators[i].values = values[i];
            }
            if (script != null) {
                script.setNextReader(context);
            }
        }

        @Override
        public void collect(int doc) throws IOException {
            for (int i = 0; i < values.length; i++) {
                values[i].forEachValueInDoc(doc, aggregators[i]);
            }
        }

        @Override
        public void postCollection() {
            long missing = 0;
            long total = 0;
            for (StaticAggregatorValueProc aggregator : aggregators) {
                missing += aggregator.missing();
                total += aggregator.total();
            }
            FieldsTermsStringFacetExecutor.this.missing = missing;
            FieldsTermsStringFacetExecutor.this.total = total;
        }
    }

    public static class AggregatorValueProc extends StaticAggregatorValueProc {

        private final ImmutableSet<BytesRef> excluded;

        private final Matcher matcher;

        private final SearchScript script;

        public AggregatorValueProc(TObjectIntHashMap<HashedBytesRef> facets, ImmutableSet<BytesRef> excluded, Pattern pattern, SearchScript script) {
            super(facets);
            this.excluded = excluded;
            this.matcher = pattern != null ? pattern.matcher("") : null;
            this.script = script;
        }

        @Override
        public void onValue(int docId, HashedBytesRef value) {
            if (excluded != null && excluded.contains(value.bytes)) {
                return;
            }

            // LUCENE 4 UPGRADE: use Lucene's RegexCapabilities
            if (matcher != null && !matcher.reset(value.bytes.utf8ToString()).matches()) {
                return;
            }
            if (script != null) {
                script.setNextDocId(docId);
                // LUCENE 4 UPGRADE: needs optimization
                script.setNextVar("term", value.bytes.utf8ToString());
                Object scriptValue = script.run();
                if (scriptValue == null) {
                    return;
                }
                if (scriptValue instanceof Boolean) {
                    if (!((Boolean) scriptValue)) {
                        return;
                    }
                } else {
                    // LUCENE 4 UPGRADE: make script return BR?
                    value = new HashedBytesRef(scriptValue.toString());
                }
            }
            super.onValue(docId, value);
        }
    }

    public static class StaticAggregatorValueProc implements HashedBytesValues.ValueInDocProc {

        // LUCENE 4 UPGRADE: check if hashcode is not too expensive
        private final TObjectIntHashMap<HashedBytesRef> facets;
        HashedBytesValues values;

        private int missing;
        private int total;

        public StaticAggregatorValueProc(TObjectIntHashMap<HashedBytesRef> facets) {
            this.facets = facets;
        }

        @Override
        public void onValue(int docId, HashedBytesRef value) {
            facets.adjustOrPutValue(values.makeSafe(value), 1, 1);
            total++;
        }

        @Override
        public void onMissing(int docId) {
            missing++;
        }

        public final TObjectIntHashMap<HashedBytesRef> facets() {
            return facets;
        }

        public final int missing() {
            return this.missing;
        }

        public final int total() {
            return this.total;
        }
    }
}
