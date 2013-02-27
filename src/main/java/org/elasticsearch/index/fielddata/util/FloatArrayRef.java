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

import com.google.common.primitives.Floats;
import org.apache.lucene.util.ArrayUtil;

import java.util.AbstractList;
import java.util.RandomAccess;

/**
 */
public class FloatArrayRef extends AbstractList<Float> implements RandomAccess {

    public static final FloatArrayRef EMPTY = new FloatArrayRef(new float[0]);

    public float[] values;
    public int start;
    public int end;

    public FloatArrayRef(float[] values) {
        this(values, 0, values.length);
    }

    public FloatArrayRef(float[] values, int length) {
        this(values, 0, length);
    }

    public FloatArrayRef(float[] values, int start, int end) {
        this.values = values;
        this.start = start;
        this.end = end;
    }

    public void reset(int newLength) {
        assert start == 0; // NOTE: senseless if offset != 0
        end = 0;
        if (values.length < newLength) {
            values = new float[ArrayUtil.oversize(newLength, 32)];
        }
    }

    @Override
    public int size() {
        return end - start;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public Float get(int index) {
        assert index < size();
        return values[start + index];
    }

    @Override
    public boolean contains(Object target) {
        // Overridden to prevent a ton of boxing
        return (target instanceof Float)
                && indexOf(values, (Float) target, start, end) != -1;
    }

    @Override
    public int indexOf(Object target) {
        // Overridden to prevent a ton of boxing
        if (target instanceof Float) {
            int i = indexOf(values, (Float) target, start, end);
            if (i >= 0) {
                return i - start;
            }
        }
        return -1;
    }

    @Override
    public int lastIndexOf(Object target) {
        // Overridden to prevent a ton of boxing
        if (target instanceof Float) {
            int i = lastIndexOf(values, (Float) target, start, end);
            if (i >= 0) {
                return i - start;
            }
        }
        return -1;
    }

    @Override
    public Float set(int index, Float element) {
        assert index < size();
        float oldValue = values[start + index];
        values[start + index] = element;
        return oldValue;
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (object instanceof FloatArrayRef) {
            FloatArrayRef that = (FloatArrayRef) object;
            int size = size();
            if (that.size() != size) {
                return false;
            }
            for (int i = 0; i < size; i++) {
                if (values[start + i] != that.values[that.start + i]) {
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
            result = 31 * result + Floats.hashCode(values[i]);
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

    private static int indexOf(float[] array, float target, int start, int end) {
        for (int i = start; i < end; i++) {
            if (array[i] == target) {
                return i;
            }
        }
        return -1;
    }

    private static int lastIndexOf(float[] array, float target, int start, int end) {
        for (int i = end - 1; i >= start; i--) {
            if (array[i] == target) {
                return i;
            }
        }
        return -1;
    }
}
