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

package org.elasticsearch.search.facet.datehistogram;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.lucene.docset.ContextDocIdSet;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;

import java.io.IOException;
import java.util.List;

/**
 * A histogram facet collector that uses different fields for the key and the value.
 */
public class ValueDateHistogramFacetExecutor extends FacetExecutor {

    private final IndexNumericFieldData keyIndexFieldData;
    private final IndexNumericFieldData valueIndexFieldData;
    private final DateHistogramFacet.ComparatorType comparatorType;
    final TimeZoneRounding tzRounding;

    final ExtTLongObjectHashMap<InternalFullDateHistogramFacet.FullEntry> entries;

    public ValueDateHistogramFacetExecutor(IndexNumericFieldData keyIndexFieldData, IndexNumericFieldData valueIndexFieldData, TimeZoneRounding tzRounding, DateHistogramFacet.ComparatorType comparatorType) {
        this.comparatorType = comparatorType;
        this.keyIndexFieldData = keyIndexFieldData;
        this.valueIndexFieldData = valueIndexFieldData;
        this.tzRounding = tzRounding;

        this.entries = CacheRecycler.popLongObjectMap();
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public Post post() {
        return new Post();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        return new InternalFullDateHistogramFacet(facetName, comparatorType, entries, true);
    }

    class Post extends FacetExecutor.Post {

        @Override
        public void executePost(List<ContextDocIdSet> docSets) throws IOException {
            DateHistogramProc histoProc = new DateHistogramProc(tzRounding, ValueDateHistogramFacetExecutor.this.entries);
            for (ContextDocIdSet docSet : docSets) {
                LongValues keyValues = keyIndexFieldData.load(docSet.context).getLongValues();
                histoProc.valueValues = valueIndexFieldData.load(docSet.context).getDoubleValues();

                DocIdSetIterator it = docSet.docSet.iterator();
                int doc;
                while ((doc = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    keyValues.forEachValueInDoc(doc, histoProc);
                }
            }
        }
    }

    class Collector extends FacetExecutor.Collector {

        private final DateHistogramProc histoProc;
        private LongValues keyValues;

        public Collector() {
            this.histoProc = new DateHistogramProc(tzRounding, entries);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            keyValues = keyIndexFieldData.load(context).getLongValues();
            histoProc.valueValues = valueIndexFieldData.load(context).getDoubleValues();
        }

        @Override
        public void collect(int doc) throws IOException {
            keyValues.forEachValueInDoc(doc, histoProc);
        }

        @Override
        public void postCollection() {
        }
    }

    public static class DateHistogramProc implements LongValues.ValueInDocProc {

        final ExtTLongObjectHashMap<InternalFullDateHistogramFacet.FullEntry> entries;
        private final TimeZoneRounding tzRounding;

        DoubleValues valueValues;

        final ValueAggregator valueAggregator = new ValueAggregator();

        public DateHistogramProc(TimeZoneRounding tzRounding, ExtTLongObjectHashMap<InternalFullDateHistogramFacet.FullEntry> entries) {
            this.tzRounding = tzRounding;
            this.entries = entries;
        }

        @Override
        public void onMissing(int docId) {
        }

        @Override
        public void onValue(int docId, long value) {
            long time = tzRounding.calc(value);

            InternalFullDateHistogramFacet.FullEntry entry = entries.get(time);
            if (entry == null) {
                entry = new InternalFullDateHistogramFacet.FullEntry(time, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0);
                entries.put(time, entry);
            }
            entry.count++;
            valueAggregator.entry = entry;
            valueValues.forEachValueInDoc(docId, valueAggregator);
        }

        public static class ValueAggregator implements DoubleValues.ValueInDocProc {

            InternalFullDateHistogramFacet.FullEntry entry;

            @Override
            public void onMissing(int docId) {
            }

            @Override
            public void onValue(int docId, double value) {
                entry.totalCount++;
                entry.total += value;
                if (value < entry.min) {
                    entry.min = value;
                }
                if (value > entry.max) {
                    entry.max = value;
                }
            }
        }
    }
}