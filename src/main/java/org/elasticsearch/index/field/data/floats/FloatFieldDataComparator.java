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

package org.elasticsearch.index.field.data.floats;

import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.support.NumericFieldDataComparator;

/**
 *
 */
// LUCENE MONITOR - Monitor against FieldComparator.Float
public class FloatFieldDataComparator extends NumericFieldDataComparator {

    private final float[] values;
    private float bottom;

    public FloatFieldDataComparator(int numHits, String fieldName, FieldDataCache fieldDataCache) {
        super(fieldName, fieldDataCache);
        values = new float[numHits];
    }

    @Override
    public FieldDataType fieldDataType() {
        return FieldDataType.DefaultTypes.FLOAT;
    }

    @Override
    public int compare(int slot1, int slot2) {
        // TODO: are there sneaky non-branch ways to compute
        // sign of float?
        final float v1 = values[slot1];
        final float v2 = values[slot2];
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
        // TODO: are there sneaky non-branch ways to compute
        // sign of float?
        final float v2 = currentFieldData.floatValue(doc);
        if (bottom > v2) {
            return 1;
        } else if (bottom < v2) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public void copy(int slot, int doc) {
        values[slot] = currentFieldData.floatValue(doc);
    }

    @Override
    public void setBottom(final int bottom) {
        this.bottom = values[bottom];
    }

    @Override
    public Comparable value(int slot) {
        return Float.valueOf(values[slot]);
    }
}
