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

package org.elasticsearch.search.facet.termsstats.doubles;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.trove.ExtTDoubleObjectHashMap;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.termsstats.TermsStatsFacet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class TermsStatsDoubleFacetExecutor extends FacetExecutor {

    private final TermsStatsFacet.ComparatorType comparatorType;

    final IndexNumericFieldData keyIndexFieldData;
    final IndexNumericFieldData valueIndexFieldData;
    final SearchScript script;

    private final int size;
    private final int numberOfShards;

    final ExtTDoubleObjectHashMap<InternalTermsStatsDoubleFacet.DoubleEntry> entries;
    long missing;

    public TermsStatsDoubleFacetExecutor(IndexNumericFieldData keyIndexFieldData, IndexNumericFieldData valueIndexFieldData, SearchScript script,
                                         int size, TermsStatsFacet.ComparatorType comparatorType, SearchContext context) {
        this.size = size;
        this.comparatorType = comparatorType;
        this.numberOfShards = context.numberOfShards();
        this.keyIndexFieldData = keyIndexFieldData;
        this.valueIndexFieldData = valueIndexFieldData;
        this.script = script;

        this.entries = CacheRecycler.popDoubleObjectMap();
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        if (entries.isEmpty()) {
            return new InternalTermsStatsDoubleFacet(facetName, comparatorType, size, ImmutableList.<InternalTermsStatsDoubleFacet.DoubleEntry>of(), missing);
        }
        if (size == 0) { // all terms
            // all terms, just return the collection, we will sort it on the way back
            return new InternalTermsStatsDoubleFacet(facetName, comparatorType, 0 /* indicates all terms*/, entries.valueCollection(), missing);
        }
        Object[] values = entries.internalValues();
        Arrays.sort(values, (Comparator) comparatorType.comparator());

        int limit = size;
        List<InternalTermsStatsDoubleFacet.DoubleEntry> ordered = Lists.newArrayList();
        for (int i = 0; i < limit; i++) {
            InternalTermsStatsDoubleFacet.DoubleEntry value = (InternalTermsStatsDoubleFacet.DoubleEntry) values[i];
            if (value == null) {
                break;
            }
            ordered.add(value);
        }

        CacheRecycler.pushDoubleObjectMap(entries);
        return new InternalTermsStatsDoubleFacet(facetName, comparatorType, size, ordered, missing);
    }

    class Collector extends FacetExecutor.Collector {

        private final Aggregator aggregator;
        private DoubleValues keyValues;

        public Collector() {
            if (script == null) {
                this.aggregator = new Aggregator(entries);
            } else {
                this.aggregator = new ScriptAggregator(entries, script);
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
            keyValues = keyIndexFieldData.load(context).getDoubleValues();
            if (script != null) {
                script.setNextReader(context);
            } else {
                aggregator.valueFieldData = valueIndexFieldData.load(context).getDoubleValues();
            }
        }

        @Override
        public void collect(int doc) throws IOException {
            keyValues.forEachValueInDoc(doc, aggregator);
        }

        @Override
        public void postCollection() {
            TermsStatsDoubleFacetExecutor.this.missing = aggregator.missing;
        }
    }

    public static class Aggregator implements DoubleValues.ValueInDocProc {

        final ExtTDoubleObjectHashMap<InternalTermsStatsDoubleFacet.DoubleEntry> entries;
        int missing;
        DoubleValues valueFieldData;
        final ValueAggregator valueAggregator = new ValueAggregator();

        public Aggregator(ExtTDoubleObjectHashMap<InternalTermsStatsDoubleFacet.DoubleEntry> entries) {
            this.entries = entries;
        }

        @Override
        public void onValue(int docId, double value) {
            InternalTermsStatsDoubleFacet.DoubleEntry doubleEntry = entries.get(value);
            if (doubleEntry == null) {
                doubleEntry = new InternalTermsStatsDoubleFacet.DoubleEntry(value, 0, 0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
                entries.put(value, doubleEntry);
            }
            doubleEntry.count++;
            valueAggregator.doubleEntry = doubleEntry;
            valueFieldData.forEachValueInDoc(docId, valueAggregator);
        }

        @Override
        public void onMissing(int docId) {
            missing++;
        }

        public static class ValueAggregator implements DoubleValues.ValueInDocProc {

            InternalTermsStatsDoubleFacet.DoubleEntry doubleEntry;

            @Override
            public void onMissing(int docId) {
            }

            @Override
            public void onValue(int docId, double value) {
                if (value < doubleEntry.min) {
                    doubleEntry.min = value;
                }
                if (value > doubleEntry.max) {
                    doubleEntry.max = value;
                }
                doubleEntry.total += value;
                doubleEntry.totalCount++;
            }
        }
    }

    public static class ScriptAggregator extends Aggregator {

        private final SearchScript script;

        public ScriptAggregator(ExtTDoubleObjectHashMap<InternalTermsStatsDoubleFacet.DoubleEntry> entries, SearchScript script) {
            super(entries);
            this.script = script;
        }

        @Override
        public void onValue(int docId, double value) {
            InternalTermsStatsDoubleFacet.DoubleEntry doubleEntry = entries.get(value);
            if (doubleEntry == null) {
                doubleEntry = new InternalTermsStatsDoubleFacet.DoubleEntry(value, 1, 0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
                entries.put(value, doubleEntry);
            } else {
                doubleEntry.count++;
            }
            script.setNextDocId(docId);
            double valueValue = script.runAsDouble();
            if (valueValue < doubleEntry.min) {
                doubleEntry.min = valueValue;
            }
            if (valueValue > doubleEntry.max) {
                doubleEntry.max = valueValue;
            }
            doubleEntry.totalCount++;
            doubleEntry.total += valueValue;
        }
    }
}