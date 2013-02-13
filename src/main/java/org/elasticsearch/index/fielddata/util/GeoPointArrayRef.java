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

package org.elasticsearch.index.fielddata.util;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.geo.GeoPoint;

import java.util.AbstractList;
import java.util.RandomAccess;

/**
 */
public class GeoPointArrayRef extends AbstractList<GeoPoint> implements RandomAccess {

    public static final GeoPointArrayRef EMPTY = new GeoPointArrayRef(new GeoPoint[0]);

    public GeoPoint[] values;
    public int start;
    public int end;

    public GeoPointArrayRef(GeoPoint[] values) {
        this(values, 0, values.length);
    }

    public GeoPointArrayRef(GeoPoint[] values, int length) {
        this(values, 0, length);
    }

    public GeoPointArrayRef(GeoPoint[] values, int start, int end) {
        this.values = values;
        this.start = start;
        this.end = end;
        for (int i = start; i < end; i++) {
            this.values[i] = new GeoPoint();
        }
    }

    public void reset(int newLength) {
        assert start == 0; // NOTE: senseless if offset != 0
        end = 0;
        if (values.length < newLength) {
            values = new GeoPoint[ArrayUtil.oversize(newLength, 32)];
            for (int i = 0; i < values.length; i++) {
                values[i] = new GeoPoint();
            }
        }
    }

    @Override
    public int size() {
        return end - start;
    }

    @Override
    public boolean isEmpty() {
        return size() != 0;
    }

    @Override
    public GeoPoint get(int index) {
        assert index >= 0 && index < size();
        return values[start + index];
    }

    @Override
    public boolean contains(Object target) {
        if (!(target instanceof GeoPoint)) {
            return false;
        }
        for (int i = start; i < end; i++) {
            if (values[i].equals((GeoPoint) target)) return true;
        }
        return false;
    }

    @Override
    public int indexOf(Object target) {
        if (!(target instanceof GeoPoint)) {
            return -1;
        }
        GeoPoint geoPoint = (GeoPoint) target;
        for (int i = start; i < end; i++) {
            if (values[i].equals(geoPoint)) return (i - start);
        }
        return -1;
    }

    @Override
    public int lastIndexOf(Object target) {
        if (!(target instanceof GeoPoint)) {
            return -1;
        }
        GeoPoint geoPoint = (GeoPoint) target;
        for (int i = end - 1; i >= start; i--) {
            if (values[i].equals(target)) return (i - start);
        }
        return -1;
    }

    @Override
    public GeoPoint set(int index, GeoPoint element) {
        assert index >= 0 && index < size();
        GeoPoint oldValue = values[start + index];
        values[start + index] = element;
        return oldValue;
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (object instanceof GeoPointArrayRef) {
            GeoPointArrayRef that = (GeoPointArrayRef) object;
            int size = size();
            if (that.size() != size) {
                return false;
            }
            for (int i = 0; i < size; i++) {
                if (!values[start + i].equals(that.values[that.start + i])) {
                    return false;
                }
            }
            return true;
        }
        return super.equals(object);
    }

    @Override
    public int hashCode() {
        int result = 1;
        for (int i = start; i < end; i++) {
            result = 31 * result + values[i].hashCode();
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(size() * 10);
        builder.append('[').append(values[start]);
        for (int i = start + 1; i < end; i++) {
            builder.append(", ").append(values[i]);
        }
        return builder.append(']').toString();
    }
}
