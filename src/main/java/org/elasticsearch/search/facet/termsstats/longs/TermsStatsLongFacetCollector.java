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

package org.elasticsearch.search.facet.termsstats.longs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.termsstats.TermsStatsFacet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class TermsStatsLongFacetCollector extends AbstractFacetCollector {

    private final TermsStatsFacet.ComparatorType comparatorType;

    private final IndexNumericFieldData keyIndexFieldData;
    private final IndexNumericFieldData valueIndexFieldData;
    private final SearchScript script;

    private final int size;
    private final int numberOfShards;

    private final Aggregator aggregator;

    private LongValues keyValues;

    public TermsStatsLongFacetCollector(String facetName, IndexNumericFieldData keyIndexFieldData, IndexNumericFieldData valueIndexFieldData, SearchScript script,
                                        int size, TermsStatsFacet.ComparatorType comparatorType, SearchContext context) {
        super(facetName);
        this.size = size;
        this.comparatorType = comparatorType;
        this.numberOfShards = context.numberOfShards();
        this.keyIndexFieldData = keyIndexFieldData;
        this.valueIndexFieldData = valueIndexFieldData;
        this.script = script;

        if (script == null) {
            this.aggregator = new Aggregator();
        } else {
            this.aggregator = new ScriptAggregator(this.script);
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
        keyValues = keyIndexFieldData.load(context).getLongValues();
        if (script != null) {
            script.setNextReader(context);
        } else {
            aggregator.valueValues = valueIndexFieldData.load(context).getDoubleValues();
        }
    }

    @Override
    protected void doCollect(int doc) throws IOException {
        keyValues.forEachValueInDoc(doc, aggregator);
    }

    @Override
    public Facet facet() {
        if (aggregator.entries.isEmpty()) {
            return new InternalTermsStatsLongFacet(facetName, comparatorType, size, ImmutableList.<InternalTermsStatsLongFacet.LongEntry>of(), aggregator.missing);
        }
        if (size == 0) { // all terms
            // all terms, just return the collection, we will sort it on the way back
            return new InternalTermsStatsLongFacet(facetName, comparatorType, 0 /* indicates all terms*/, aggregator.entries.valueCollection(), aggregator.missing);
        }

        // we need to fetch facets of "size * numberOfShards" because of problems in how they are distributed across shards
        Object[] values = aggregator.entries.internalValues();
        Arrays.sort(values, (Comparator) comparatorType.comparator());

        int limit = size;
        List<InternalTermsStatsLongFacet.LongEntry> ordered = Lists.newArrayList();
        for (int i = 0; i < limit; i++) {
            InternalTermsStatsLongFacet.LongEntry value = (InternalTermsStatsLongFacet.LongEntry) values[i];
            if (value == null) {
                break;
            }
            ordered.add(value);
        }
        CacheRecycler.pushLongObjectMap(aggregator.entries);
        return new InternalTermsStatsLongFacet(facetName, comparatorType, size, ordered, aggregator.missing);
    }

    public static class Aggregator implements LongValues.ValueInDocProc {

        final ExtTLongObjectHashMap<InternalTermsStatsLongFacet.LongEntry> entries = CacheRecycler.popLongObjectMap();

        int missing;

        DoubleValues valueValues;

        final ValueAggregator valueAggregator = new ValueAggregator();

        @Override
        public void onValue(int docId, long value) {
            InternalTermsStatsLongFacet.LongEntry longEntry = entries.get(value);
            if (longEntry == null) {
                longEntry = new InternalTermsStatsLongFacet.LongEntry(value, 0, 0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
                entries.put(value, longEntry);
            }
            longEntry.count++;
            valueAggregator.longEntry = longEntry;
            valueValues.forEachValueInDoc(docId, valueAggregator);
        }

        @Override
        public void onMissing(int docId) {
            missing++;
        }

        public static class ValueAggregator implements DoubleValues.ValueInDocProc {

            InternalTermsStatsLongFacet.LongEntry longEntry;

            @Override
            public void onMissing(int docId) {
            }

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

        public ScriptAggregator(SearchScript script) {
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