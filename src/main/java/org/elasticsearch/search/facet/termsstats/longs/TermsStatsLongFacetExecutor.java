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

package org.elasticsearch.search.facet.termsstats.longs;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.DoubleFacetAggregatorBase;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.LongFacetAggregatorBase;
import org.elasticsearch.search.facet.termsstats.TermsStatsFacet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class TermsStatsLongFacetExecutor extends FacetExecutor {

    private final TermsStatsFacet.ComparatorType comparatorType;
    final IndexNumericFieldData keyIndexFieldData;
    final IndexNumericFieldData valueIndexFieldData;
    final SearchScript script;

    private final int size;
    private final int shardSize;

    final Recycler.V<LongObjectOpenHashMap<InternalTermsStatsLongFacet.LongEntry>> entries;
    long missing;

    public TermsStatsLongFacetExecutor(IndexNumericFieldData keyIndexFieldData, IndexNumericFieldData valueIndexFieldData, SearchScript script,
                                       int size, int shardSize, TermsStatsFacet.ComparatorType comparatorType, SearchContext context) {
        this.size = size;
        this.shardSize = shardSize;
        this.comparatorType = comparatorType;
        this.keyIndexFieldData = keyIndexFieldData;
        this.valueIndexFieldData = valueIndexFieldData;
        this.script = script;

        this.entries = context.cacheRecycler().longObjectMap(-1);
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        if (entries.v().isEmpty()) {
            entries.close();
            return new InternalTermsStatsLongFacet(facetName, comparatorType, size, ImmutableList.<InternalTermsStatsLongFacet.LongEntry>of(), missing);
        }
        if (size == 0) { // all terms
            // all terms, just return the collection, we will sort it on the way back
            List<InternalTermsStatsLongFacet.LongEntry> longEntries = new ArrayList<>(entries.v().size());
            boolean[] states = entries.v().allocated;
            Object[] values = entries.v().values;
            for (int i = 0; i < states.length; i++) {
                if (states[i]) {
                    longEntries.add((InternalTermsStatsLongFacet.LongEntry) values[i]);
                }
            }

            entries.close();
            return new InternalTermsStatsLongFacet(facetName, comparatorType, 0 /* indicates all terms*/, longEntries, missing);
        }

        // we need to fetch facets of "size * numberOfShards" because of problems in how they are distributed across shards
        Object[] values = entries.v().values;
        Arrays.sort(values, (Comparator) comparatorType.comparator());

        int limit = shardSize;
        List<InternalTermsStatsLongFacet.LongEntry> ordered = Lists.newArrayList();
        for (int i = 0; i < limit; i++) {
            InternalTermsStatsLongFacet.LongEntry value = (InternalTermsStatsLongFacet.LongEntry) values[i];
            if (value == null) {
                break;
            }
            ordered.add(value);
        }
        entries.close();
        return new InternalTermsStatsLongFacet(facetName, comparatorType, size, ordered, missing);
    }

    class Collector extends FacetExecutor.Collector {

        private final Aggregator aggregator;
        private SortedNumericDocValues keyValues;

        public Collector() {
            if (script == null) {
                this.aggregator = new Aggregator(entries.v());
            } else {
                this.aggregator = new ScriptAggregator(entries.v(), script);
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
            keyValues = keyIndexFieldData.load(context).getLongValues();
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
            TermsStatsLongFacetExecutor.this.missing = aggregator.missing();
        }
    }

    public static class Aggregator extends LongFacetAggregatorBase {

        final LongObjectOpenHashMap<InternalTermsStatsLongFacet.LongEntry> entries;
        SortedNumericDoubleValues valueValues;
        final ValueAggregator valueAggregator = new ValueAggregator();

        public Aggregator(LongObjectOpenHashMap<InternalTermsStatsLongFacet.LongEntry> entries) {
            this.entries = entries;
        }

        @Override
        public void onValue(int docId, long value) {
            InternalTermsStatsLongFacet.LongEntry longEntry = entries.get(value);
            if (longEntry == null) {
                longEntry = new InternalTermsStatsLongFacet.LongEntry(value, 0, 0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
                entries.put(value, longEntry);
            }
            longEntry.count++;
            valueAggregator.longEntry = longEntry;
            valueAggregator.onDoc(docId, valueValues);
        }


        public final static class ValueAggregator extends DoubleFacetAggregatorBase {

            InternalTermsStatsLongFacet.LongEntry longEntry;

            @Override
            public void onValue(int docId, double value) {
                if (value < longEntry.min) {
                    longEntry.min = value;
                }
                if (value > longEntry.max) {
                    longEntry.max = value;
                }
                longEntry.total += value;
                longEntry.totalCount++;
            }
        }
    }

    public static class ScriptAggregator extends Aggregator {

        private final SearchScript script;

        public ScriptAggregator(LongObjectOpenHashMap<InternalTermsStatsLongFacet.LongEntry> entries, SearchScript script) {
            super(entries);
            this.script = script;
        }

        @Override
        public void onValue(int docId, long value) {
            InternalTermsStatsLongFacet.LongEntry longEntry = entries.get(value);
            if (longEntry == null) {
                longEntry = new InternalTermsStatsLongFacet.LongEntry(value, 1, 0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
                entries.put(value, longEntry);
            } else {
                longEntry.count++;
            }
            script.setNextDocId(docId);
            double valueValue = script.runAsDouble();
            if (valueValue < longEntry.min) {
                longEntry.min = valueValue;
            }
            if (valueValue > longEntry.max) {
                longEntry.max = valueValue;
            }
            longEntry.totalCount++;
            longEntry.total += valueValue;
        }
    }
}