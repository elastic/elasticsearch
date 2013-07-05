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

package org.elasticsearch.search.facet.statistical;

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
public class StatisticalFieldsFacetExecutor extends FacetExecutor {

    private final IndexNumericFieldData[] indexFieldDatas;

    double min = Double.POSITIVE_INFINITY;
    double max = Double.NEGATIVE_INFINITY;
    double total = 0;
    double sumOfSquares = 0.0;
    long count;
    int missing;

    public StatisticalFieldsFacetExecutor(IndexNumericFieldData[] indexFieldDatas, SearchContext context) {
        this.indexFieldDatas = indexFieldDatas;
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        return new InternalStatisticalFacet(facetName, min, max, total, sumOfSquares, count);
    }

    class Collector extends FacetExecutor.Collector {

        private final StatsProc statsProc = new StatsProc();
        private DoubleValues[] values;

        public Collector() {
            this.values = new DoubleValues[indexFieldDatas.length];
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            for (int i = 0; i < indexFieldDatas.length; i++) {
                values[i] = indexFieldDatas[i].load(context).getDoubleValues();
            }
        }

        @Override
        public void collect(int doc) throws IOException {
            for (DoubleValues value : values) {
                statsProc.onDoc(doc, value);
            }
        }

        @Override
        public void postCollection() {
            StatisticalFieldsFacetExecutor.this.min = statsProc.min;
            StatisticalFieldsFacetExecutor.this.max = statsProc.max;
            StatisticalFieldsFacetExecutor.this.total = statsProc.sum;
            StatisticalFieldsFacetExecutor.this.sumOfSquares = statsProc.sumOfSquares;
            StatisticalFieldsFacetExecutor.this.count = statsProc.count;
            StatisticalFieldsFacetExecutor.this.missing = statsProc.missing;
        }
    }

    public static class StatsProc extends DoubleFacetAggregatorBase {

        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        double sum = 0;
        double sumOfSquares = 0.0;
        long count;
        int missing;

        @Override
        public void onValue(int docId, double value) {
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
            sumOfSquares += value * value;
            sum += value;
            count++;
        }
    }
}
