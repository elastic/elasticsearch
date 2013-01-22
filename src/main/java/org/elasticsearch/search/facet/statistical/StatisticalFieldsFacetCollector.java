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
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class StatisticalFieldsFacetCollector extends AbstractFacetCollector {

    private final IndexNumericFieldData[] indexFieldDatas;

    private DoubleValues[] values;

    private final StatsProc statsProc = new StatsProc();

    public StatisticalFieldsFacetCollector(String facetName, IndexNumericFieldData[] indexFieldDatas, SearchContext context) {
        super(facetName);
        this.indexFieldDatas = indexFieldDatas;
        this.values = new DoubleValues[indexFieldDatas.length];
    }

    @Override
    protected void doCollect(int doc) throws IOException {
        for (DoubleValues value : values) {
            value.forEachValueInDoc(doc, statsProc);
        }
    }

    @Override
    protected void doSetNextReader(AtomicReaderContext context) throws IOException {
        for (int i = 0; i < indexFieldDatas.length; i++) {
            values[i] = indexFieldDatas[i].load(context).getDoubleValues();
        }
    }

    @Override
    public Facet facet() {
        return new InternalStatisticalFacet(facetName, statsProc.min(), statsProc.max(), statsProc.total(), statsProc.sumOfSquares(), statsProc.count());
    }

    public static class StatsProc implements DoubleValues.ValueInDocProc {

        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        double total = 0;
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
            total += value;
            count++;
        }

        @Override
        public void onMissing(int docId) {
            missing++;
        }

        public final double min() {
            return min;
        }

        public final double max() {
            return max;
        }

        public final double total() {
            return total;
        }

        public final long count() {
            return count;
        }

        public final double sumOfSquares() {
            return sumOfSquares;
        }
    }
}
