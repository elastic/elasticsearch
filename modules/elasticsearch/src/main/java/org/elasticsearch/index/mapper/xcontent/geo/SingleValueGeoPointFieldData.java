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

package org.elasticsearch.index.mapper.xcontent.geo;

import org.elasticsearch.common.thread.ThreadLocals;

/**
 * @author kimchy (shay.banon)
 */
public class SingleValueGeoPointFieldData extends GeoPointFieldData {

    private static ThreadLocal<ThreadLocals.CleanableValue<GeoPoint[]>> valuesCache = new ThreadLocal<ThreadLocals.CleanableValue<GeoPoint[]>>() {
        @Override protected ThreadLocals.CleanableValue<GeoPoint[]> initialValue() {
            return new ThreadLocals.CleanableValue<GeoPoint[]>(new GeoPoint[1]);
        }
    };

    // order with value 0 indicates no value
    private final int[] order;

    public SingleValueGeoPointFieldData(String fieldName, int[] order, GeoPoint[] values) {
        super(fieldName, values);
        this.order = order;
    }

    int[] order() {
        return order;
    }

    GeoPoint[] values() {
        return this.values;
    }

    @Override public boolean multiValued() {
        return false;
    }

    @Override public boolean hasValue(int docId) {
        return order[docId] != 0;
    }

    @Override public void forEachValueInDoc(int docId, StringValueInDocProc proc) {
        int loc = order[docId];
        if (loc == 0) {
            return;
        }
        proc.onValue(docId, values[loc].geohash());
    }

    @Override public GeoPoint value(int docId) {
        return values[order[docId]];
    }

    @Override public GeoPoint[] values(int docId) {
        int loc = order[docId];
        if (loc == 0) {
            return EMPTY_ARRAY;
        }
        GeoPoint[] ret = valuesCache.get().get();
        ret[0] = values[loc];
        return ret;
    }
}
