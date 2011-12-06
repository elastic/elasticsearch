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

package org.elasticsearch.index.field.data.longs;

import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.support.NumericFieldDataComparator;

/**
 *
 */
// LUCENE MONITOR - Monitor against FieldComparator.Long
public class LongFieldDataComparator extends NumericFieldDataComparator {

    private final long[] values;
    private long bottom;

    public LongFieldDataComparator(int numHits, String fieldName, FieldDataCache fieldDataCache) {
        super(fieldName, fieldDataCache);
        values = new long[numHits];
    }

    @Override
    public FieldDataType fieldDataType() {
        return FieldDataType.DefaultTypes.LONG;
    }

    @Override
    public int compare(int slot1, int slot2) {
        // TODO: there are sneaky non-branch ways to compute
        // -1/+1/0 sign
        final long v1 = values[slot1];
        final long v2 = values[slot2];
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
        // TODO: there are sneaky non-branch ways to compute
        // -1/+1/0 sign
//        final long v2 = currentReaderValues[doc];
        final long v2 = currentFieldData.longValue(doc);
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
        values[slot] = currentFieldData.longValue(doc);
    }

    @Override
    public void setBottom(final int bottom) {
        this.bottom = values[bottom];
    }

    @Override
    public Comparable value(int slot) {
        return Long.valueOf(values[slot]);
    }

}
