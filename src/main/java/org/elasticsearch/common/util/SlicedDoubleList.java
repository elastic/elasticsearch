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
package org.elasticsearch.common.util;

import java.util.AbstractList;
import java.util.RandomAccess;

import org.apache.lucene.util.ArrayUtil;

import com.google.common.primitives.Doubles;

public final class SlicedDoubleList extends AbstractList<Double> implements RandomAccess {

    public static final SlicedDoubleList EMPTY = new SlicedDoubleList(0);

    public double[] values;
    public int offset;
    public int length;

    public SlicedDoubleList(int capacity) {
        this(new double[capacity], 0, capacity);
    }

    public SlicedDoubleList(double[] values, int offset, int length) {
        this.values = values;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public int size() {
        return length;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public Double get(int index) {
        assert index < size();
        return values[offset + index];
    }

    @Override
    public boolean contains(Object target) {
        // Overridden to prevent a ton of boxing
        return (target instanceof Double)
                && indexOf(values, (Double) target, offset, offset+length) != -1;
    }

    @Override
    public int indexOf(Object target) {
        // Overridden to prevent a ton of boxing
        if (target instanceof Double) {
            int i = indexOf(values, (Double) target, offset, offset+length);
            if (i >= 0) {
                return i - offset;
            }
        }
        return -1;
    }

    @Override
    public int lastIndexOf(Object target) {
        // Overridden to prevent a ton of boxing
        if (target instanceof Double) {
            int i = lastIndexOf(values, (Double) target, offset, offset+length);
            if (i >= 0) {
                return i - offset;
            }
        }
        return -1;
    }

    @Override
    public Double set(int index, Double element) {
        throw new UnsupportedOperationException("modifying list opertations are not implemented");
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (object instanceof SlicedDoubleList) {
            SlicedDoubleList that = (SlicedDoubleList) object;
            int size = size();
            if (that.size() != size) {
                return false;
            }
            for (int i = 0; i < size; i++) {
                if (values[offset + i] != that.values[that.offset + i]) {
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
        for (int i = 0; i < length; i++) {
            result = 31 * result + Doubles.hashCode(values[offset+i]);
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(size() * 10);
        builder.append('[');
        if (length > 0) {
          builder.append(values[offset]);
            for (int i = 1; i < length; i++) {
                builder.append(", ").append(values[offset+i]);
            }
        }
        return builder.append(']').toString();
    }

    private static int indexOf(double[] array, double target, int start, int end) {
        for (int i = start; i < end; i++) {
            if (array[i] == target) {
                return i;
            }
        }
        return -1;
    }

    private static int lastIndexOf(double[] array, double target, int start, int end) {
        for (int i = end - 1; i >= start; i--) {
            if (array[i] == target) {
                return i;
            }
        }
        return -1;
    }
    
    public void grow(int newLength) {
        assert offset == 0;
        values = ArrayUtil.grow(values, newLength);
    }
}