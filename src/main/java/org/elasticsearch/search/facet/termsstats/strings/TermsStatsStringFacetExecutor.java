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

package org.elasticsearch.search.facet.termsstats.strings;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.DoubleFacetAggregatorBase;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.terms.strings.HashedAggregator;
import org.elasticsearch.search.facet.termsstats.TermsStatsFacet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class TermsStatsStringFacetExecutor extends FacetExecutor {

    private final TermsStatsFacet.ComparatorType comparatorType;
    final IndexFieldData keyIndexFieldData;
    final IndexNumericFieldData valueIndexFieldData;
    final SearchScript script;
    private final int size;
    private final int shardSize;

    final Recycler.V<ObjectObjectOpenHashMap<HashedBytesRef, InternalTermsStatsStringFacet.StringEntry>> entries;
    long missing;

    public TermsStatsStringFacetExecutor(IndexFieldData keyIndexFieldData, IndexNumericFieldData valueIndexFieldData, SearchScript valueScript,
                                         int size, int shardSize, TermsStatsFacet.ComparatorType comparatorType, SearchContext context) {
        this.keyIndexFieldData = keyIndexFieldData;
        this.valueIndexFieldData = valueIndexFieldData;
        this.script = valueScript;
        this.size = size;
        this.shardSize = shardSize;
        this.comparatorType = comparatorType;

        this.entries = context.cacheRecycler().hashMap(-1);
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        if (entries.v().isEmpty()) {
            entries.release();
            return new InternalTermsStatsStringFacet(facetName, comparatorType, size, ImmutableList.<InternalTermsStatsStringFacet.StringEntry>of(), missing);
        }
        if (size == 0) { // all terms
            // all terms, just return the collection, we will sort it on the way back
            List<InternalTermsStatsStringFacet.StringEntry> stringEntries = new ArrayList<InternalTermsStatsStringFacet.StringEntry>();
            final boolean[] states = entries.v().allocated;
            final Object[] values = entries.v().values;
            for (int i = 0; i < states.length; i++) {
                if (states[i]) {
                    stringEntries.add((InternalTermsStatsStringFacet.StringEntry) values[i]);
                }
            }
            return new InternalTermsStatsStringFacet(facetName, comparatorType, 0 /* indicates all terms*/, stringEntries, missing);
        }
        Object[] values = entries.v().values;
        Arrays.sort(values, (Comparator) comparatorType.comparator());

        List<InternalTermsStatsStringFacet.StringEntry> ordered = Lists.newArrayList();
        int limit = shardSize;
        for (int i = 0; i < limit; i++) {
            InternalTermsStatsStringFacet.StringEntry value = (InternalTermsStatsStringFacet.StringEntry) values[i];
            if (value == null) {
                break;
            }
            ordered.add(value);
        }

        entries.release();
        return new InternalTermsStatsStringFacet(facetName, comparatorType, size, ordered, missing);
    }

    class Collector extends FacetExecutor.Collector {

        private final Aggregator aggregator;
        private BytesValues keyValues;

        public Collector() {
            if (script != null) {
                this.aggregator = new ScriptAggregator(entries.v(), script);
            } else {
                this.aggregator = new Aggregator(entries.v());
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
            keyValues = keyIndexFieldData.load(context).getBytesValues(true);
            if (script != null) {
                script.setNextReader(context);
            } else {
                aggregator.valueValues = valueIndexFieldData.load(context).getDoubleValues();
            }
        }

        @Override
        public void collect(int doc) throws IOException {
            aggregator.onDoc(doc, keyValues);
        }

        @Override
        public void postCollection() {
            TermsStatsStringFacetExecutor.this.missing = aggregator.missing;
            aggregator.release();
        }
    }

    public static class Aggregator extends HashedAggregator {

        final ObjectObjectOpenHashMap<HashedBytesRef, InternalTermsStatsStringFacet.StringEntry> entries;
        final HashedBytesRef spare = new HashedBytesRef();
        int missing = 0;

        DoubleValues valueValues;

        ValueAggregator valueAggregator = new ValueAggregator();

        public Aggregator(ObjectObjectOpenHashMap<HashedBytesRef, InternalTermsStatsStringFacet.StringEntry> entries) {
            this.entries = entries;
        }

        @Override
        public void onValue(int docId, BytesRef value, int hashCode, BytesValues values) {
            spare.reset(value, hashCode);
            InternalTermsStatsStringFacet.StringEntry stringEntry = entries.get(spare);
            if (stringEntry == null) {
                HashedBytesRef theValue = new HashedBytesRef(values.copyShared(), hashCode);
                stringEntry = new InternalTermsStatsStringFacet.StringEntry(theValue, 0, 0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
                entries.put(theValue, stringEntry);
            }
            stringEntry.count++;
            valueAggregator.stringEntry = stringEntry;
            valueAggregator.onDoc(docId, valueValues);
        }

        public static class ValueAggregator extends DoubleFacetAggregatorBase {

            InternalTermsStatsStringFacet.StringEntry stringEntry;

            @Override
            public void onValue(int docId, double value) {
                if (value < stringEntry.min) {
                    stringEntry.min = value;
                }
                if (value > stringEntry.max) {
                    stringEntry.max = value;
                }
                stringEntry.total += value;
                stringEntry.totalCount++;
            }
        }
    }

    public static class ScriptAggregator extends Aggregator {
        private final SearchScript script;

        public ScriptAggregator(ObjectObjectOpenHashMap<HashedBytesRef, InternalTermsStatsStringFacet.StringEntry> entries, SearchScript script) {
            super(entries);
            this.script = script;
        }

        @Override
        public void onValue(int docId, BytesRef value, int hashCode, BytesValues values) {
            spare.reset(value, hashCode);
            InternalTermsStatsStringFacet.StringEntry stringEntry = entries.get(spare);
            if (stringEntry == null) {
                HashedBytesRef theValue = new HashedBytesRef(values.copyShared(), hashCode);
                stringEntry = new InternalTermsStatsStringFacet.StringEntry(theValue, 1, 0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
                entries.put(theValue, stringEntry);
            } else {
                stringEntry.count++;
            }

            script.setNextDocId(docId);
            double valueValue = script.runAsDouble();
            if (valueValue < stringEntry.min) {
                stringEntry.min = valueValue;
            }
            if (valueValue > stringEntry.max) {
                stringEntry.max = valueValue;
            }
            stringEntry.total += valueValue;
            stringEntry.totalCount++;
        }
    }
}