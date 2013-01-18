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

import java.util.AbstractList;
import java.util.RandomAccess;

/**
 */
public class StringArrayRef extends AbstractList<String> implements RandomAccess {

    public static final StringArrayRef EMPTY = new StringArrayRef(new String[0]);

    public String[] values;
    public int start;
    public int end;

    public StringArrayRef(String[] values) {
        this(values, 0, values.length);
    }

    public StringArrayRef(String[] values, int length) {
        this(values, 0, length);
    }

    public StringArrayRef(String[] values, int start, int end) {
        this.values = values;
        this.start = start;
        this.end = end;
    }

    public void reset(int newLength) {
        assert start == 0; // NOTE: senseless if offset != 0
        end = 0;
        if (values.length < newLength) {
            values = new String[ArrayUtil.oversize(newLength, 32)];
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
    public String get(int index) {
        assert index >= 0 && index < size();
        return values[start + index];
    }

    @Override
    public boolean contains(Object target) {
        String sTarget = target.toString();
        for (int i = start; i < end; i++) {
            if (values[i].equals(sTarget)) return true;
        }
        return false;
    }

    @Override
    public int indexOf(Object target) {
        String sTarget = target.toString();
        for (int i = start; i < end; i++) {
            if (values[i].equals(sTarget)) return (i - start);
        }
        return -1;
    }

    @Override
    public int lastIndexOf(Object target) {
        String sTarget = target.toString();
        for (int i = end - 1; i >= start; i--) {
            if (values[i].equals(sTarget)) return (i - start);
        }
        return -1;
    }

    @Override
    public String set(int index, String element) {
        assert index >= 0 && index < size();
        String oldValue = values[start + index];
        values[start + index] = element;
        return oldValue;
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (object instanceof StringArrayRef) {
            StringArrayRef that = (StringArrayRef) object;
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
