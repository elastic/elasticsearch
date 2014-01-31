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

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SortField;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.script.SearchScript;

import java.io.IOException;

/**
 *
 */
// LUCENE MONITOR: Monitor against FieldComparator.Double
public class DoubleScriptDataComparator extends NumberComparatorBase<Double> {

    public static IndexFieldData.XFieldComparatorSource comparatorSource(SearchScript script) {
        return new InnerSource(script);
    }

    private static class InnerSource extends IndexFieldData.XFieldComparatorSource {

        private final SearchScript script;

        private InnerSource(SearchScript script) {
            this.script = script;
        }

        @Override
        public FieldComparator<? extends Number> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
            return new DoubleScriptDataComparator(numHits, script);
        }

        @Override
        public SortField.Type reducedType() {
            return SortField.Type.DOUBLE;
        }
    }

    private final SearchScript script;

    private final double[] values;
    private double bottom;

    public DoubleScriptDataComparator(int numHits, SearchScript script) {
        this.script = script;
        values = new double[numHits];
    }

    @Override
    public FieldComparator<Double> setNextReader(AtomicReaderContext context) throws IOException {
        script.setNextReader(context);
        return this;
    }

    @Override
    public void setScorer(Scorer scorer) {
        script.setScorer(scorer);
    }

    @Override
    public int compare(int slot1, int slot2) {
        final double v1 = values[slot1];
        final double v2 = values[slot2];
        if (v1 > v2) {
            return 1;
        } else if (v1 < v2) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public int compareBottom(int doc) {
        script.setNextDocId(doc);
        final double v2 = script.runAsDouble();
        if (bottom > v2) {
            return 1;
        } else if (bottom < v2) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public int compareTop(int doc) throws IOException {
        script.setNextDocId(doc);
        double docValue = script.runAsDouble();
        return Double.compare(top, docValue);
    }

    @Override
    public void copy(int slot, int doc) {
        script.setNextDocId(doc);
        values[slot] = script.runAsDouble();
    }

    @Override
    public void setBottom(final int bottom) {
        this.bottom = values[bottom];
    }

    @Override
    public Double value(int slot) {
        return values[slot];
    }

    @Override
    public void add(int slot, int doc) {
        script.setNextDocId(doc);
        values[slot] += script.runAsDouble();
    }

    @Override
    public void divide(int slot, int divisor) {
        values[slot] /= divisor;
    }

    @Override
    public void missing(int slot) {
        values[slot] = Double.MAX_VALUE;
    }

    @Override
    public int compareBottomMissing() {
        return Double.compare(bottom, Double.MAX_VALUE);
    }

    @Override
    public int compareTopMissing() {
        return Double.compare(top, Double.MAX_VALUE);
    }
}
