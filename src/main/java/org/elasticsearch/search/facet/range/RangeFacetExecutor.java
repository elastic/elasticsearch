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

package org.elasticsearch.search.facet.range;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.search.facet.DoubleFacetAggregatorBase;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class RangeFacetExecutor extends FacetExecutor {

    private final IndexNumericFieldData indexFieldData;

    private final RangeFacet.Entry[] entries;

    public RangeFacetExecutor(IndexNumericFieldData indexFieldData, RangeFacet.Entry[] entries, SearchContext context) {
        this.indexFieldData = indexFieldData;
        this.entries = entries;
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        return new InternalRangeFacet(facetName, entries);
    }

    class Collector extends FacetExecutor.Collector {

        private final RangeProc rangeProc;
        private DoubleValues values;

        public Collector() {
            rangeProc = new RangeProc(entries);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            values = indexFieldData.load(context).getDoubleValues();
        }

        @Override
        public void collect(int doc) throws IOException {
            for (RangeFacet.Entry entry : entries) {
                entry.foundInDoc = false;
            }
            rangeProc.onDoc(doc, values);
        }

        @Override
        public void postCollection() {
        }
    }

    public static class RangeProc extends DoubleFacetAggregatorBase{

        private final RangeFacet.Entry[] entries;

        public RangeProc(RangeFacet.Entry[] entries) {
            this.entries = entries;
        }

        @Override
        public void onValue(int docId, double value) {
            for (RangeFacet.Entry entry : entries) {
                if (entry.foundInDoc) {
                    continue;
                }
                if (value >= entry.getFrom() && value < entry.getTo()) {
                    entry.foundInDoc = true;
                    entry.count++;
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
}
