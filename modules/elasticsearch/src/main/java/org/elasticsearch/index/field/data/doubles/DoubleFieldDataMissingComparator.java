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

package org.elasticsearch.index.field.data.doubles;

import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.support.NumericFieldDataComparator;

/**
 * @author kimchy (shay.banon)
 */
// LUCENE MONITOR: Monitor against FieldComparator.Double
public class DoubleFieldDataMissingComparator extends NumericFieldDataComparator {

    private final double[] values;
    private double bottom;
    private final double missingValue;

    public DoubleFieldDataMissingComparator(int numHits, String fieldName, FieldDataCache fieldDataCache, double missingValue) {
        super(fieldName, fieldDataCache);
        values = new double[numHits];
        this.missingValue = missingValue;
    }

    @Override public FieldDataType fieldDataType() {
        return FieldDataType.DefaultTypes.DOUBLE;
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
        double v2 = missingValue;
        if (currentFieldData.hasValue(doc)) {
            v2 = currentFieldData.doubleValue(doc);
        }
        if (bottom > v2) {
            return 1;
        } else if (bottom < v2) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override public void copy(int slot, int doc) {
        double value = missingValue;
        if (currentFieldData.hasValue(doc)) {
            value = currentFieldData.doubleValue(doc);
        }
        values[slot] = value;
    }

    @Override public void setBottom(final int bottom) {
        this.bottom = values[bottom];
    }

    @Override public Comparable value(int slot) {
        return Double.valueOf(values[slot]);
    }
}
