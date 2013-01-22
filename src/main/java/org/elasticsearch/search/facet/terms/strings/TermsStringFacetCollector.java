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
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class TermsStringFacetCollector extends AbstractFacetCollector {

    private final IndexFieldData indexFieldData;

    private final TermsFacet.ComparatorType comparatorType;

    private final int size;

    private final int numberOfShards;

    private HashedBytesValues values;

    private final StaticAggregatorValueProc aggregator;

    private final SearchScript script;

    public TermsStringFacetCollector(String facetName, IndexFieldData indexFieldData, int size, TermsFacet.ComparatorType comparatorType, boolean allTerms, SearchContext context,
                                     ImmutableSet<BytesRef> excluded, Pattern pattern, SearchScript script) {
        super(facetName);
        this.indexFieldData = indexFieldData;
        this.size = size;
        this.comparatorType = comparatorType;
        this.numberOfShards = context.numberOfShards();
        this.script = script;

        if (excluded.isEmpty() && pattern == null && this.script == null) {
            aggregator = new StaticAggregatorValueProc(CacheRecycler.<HashedBytesRef>popObjectIntMap());
        } else {
            aggregator = new AggregatorValueProc(CacheRecycler.<HashedBytesRef>popObjectIntMap(), excluded, pattern, this.script);
        }

        if (allTerms) {
            // TODO: we need to support this back with the new field data!
//            try {
//                for (AtomicReaderContext readerContext : context.searcher().getTopReaderContext().leaves()) {
//                    FieldData fieldData = fieldDataCache.cache(fieldDataType, readerContext.reader(), indexFieldName);
//                    fieldData.forEachValue(aggregator);
//                }
//            } catch (Exception e) {
//                throw new FacetPhaseExecutionException(facetName, "failed to load all terms", e);
//            }
        }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        if (script != null) {
            script.setScorer(scorer);
        }
    }

    @Override
    protected void doSetNextReader(AtomicReaderContext context) throws IOException {
        values = indexFieldData.load(context).getHashedBytesValues();
        aggregator.values = values;
        if (script != null) {
            script.setNextReader(context);
        }
    }

    @Override
    protected void doCollect(int doc) throws IOException {
        values.forEachValueInDoc(doc, aggregator);
    }

    @Override
    public Facet facet() {
        TObjectIntHashMap<HashedBytesRef> facets = aggregator.facets();
        if (facets.isEmpty()) {
            CacheRecycler.pushObjectIntMap(facets);
            return new InternalStringTermsFacet(facetName, comparatorType, size, ImmutableList.<InternalStringTermsFacet.TermEntry>of(), aggregator.missing(), aggregator.total());
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
                return new InternalStringTermsFacet(facetName, comparatorType, size, Arrays.asList(list), aggregator.missing(), aggregator.total());
            } else {
                BoundedTreeSet<InternalStringTermsFacet.TermEntry> ordered = new BoundedTreeSet<InternalStringTermsFacet.TermEntry>(comparatorType.comparator(), size);
                for (TObjectIntIterator<HashedBytesRef> it = facets.iterator(); it.hasNext(); ) {
                    it.advance();
                    ordered.add(new InternalStringTermsFacet.TermEntry(it.key().bytes, it.value()));
                }
                CacheRecycler.pushObjectIntMap(facets);
                return new InternalStringTermsFacet(facetName, comparatorType, size, ordered, aggregator.missing(), aggregator.total());
            }
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
                    // LUCENE 4 UPGRADE: should be possible to convert directly to BR
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
        private int missing = 0;
        private int total = 0;

        public StaticAggregatorValueProc(TObjectIntHashMap<HashedBytesRef> facets) {
            this.facets = facets;
        }

        @Override
        public void onValue(int docId, HashedBytesRef value) {
            // we have to "makeSafe", even if it exists, since it might not..., need to find a way to optimize it
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

        public int total() {
            return this.total;
        }
    }
}
