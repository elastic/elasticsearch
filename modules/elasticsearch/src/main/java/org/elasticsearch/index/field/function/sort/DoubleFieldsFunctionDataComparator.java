/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.field.function.sort;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.elasticsearch.index.field.function.FieldsFunction;

import java.io.IOException;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
// LUCENE MONITOR: Monitor against FieldComparator.Double
public class DoubleFieldsFunctionDataComparator extends FieldComparator {

    public static FieldComparatorSource comparatorSource(FieldsFunction fieldsFunction, Map<String, Object> params) {
        return new InnerSource(fieldsFunction, params);
    }

    private static class InnerSource extends FieldComparatorSource {

        private final FieldsFunction fieldsFunction;

        private final Map<String, Object> params;

        private InnerSource(FieldsFunction fieldsFunction, Map<String, Object> params) {
            this.fieldsFunction = fieldsFunction;
            this.params = params;
        }

        @Override public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
            return new DoubleFieldsFunctionDataComparator(numHits, fieldsFunction, params);
        }
    }

    private final FieldsFunction fieldsFunction;

    private final Map<String, Object> params;

    private final double[] values;
    private double bottom;

    public DoubleFieldsFunctionDataComparator(int numHits, FieldsFunction fieldsFunction, Map<String, Object> params) {
        this.fieldsFunction = fieldsFunction;
        this.params = params;
        values = new double[numHits];
    }

    @Override public void setNextReader(IndexReader reader, int docBase) throws IOException {
        fieldsFunction.setNextReader(reader);
    }

    @Override public int compare(int slot1, int slot2) {
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

    @Override public int compareBottom(int doc) {
        final double v2 = ((Number) fieldsFunction.execute(doc, params)).doubleValue();
        if (bottom > v2) {
            return 1;
        } else if (bottom < v2) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override public void copy(int slot, int doc) {
        values[slot] = ((Number) fieldsFunction.execute(doc, params)).doubleValue();
    }

    @Override public void setBottom(final int bottom) {
        this.bottom = values[bottom];
    }

    @Override public Comparable value(int slot) {
        return Double.valueOf(values[slot]);
    }
}
