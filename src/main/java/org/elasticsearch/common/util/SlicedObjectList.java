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
package org.elasticsearch.common.util;

import java.util.AbstractList;
import java.util.RandomAccess;

// TODO this could use some javadocs
public abstract class SlicedObjectList<T> extends AbstractList<T> implements RandomAccess {

    public T[] values;
    public int offset;
    public int length;

    public SlicedObjectList(T[] values) {
        this(values, 0, values.length);
    }
    
    public SlicedObjectList(T[] values, int offset, int length) {
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
    public T get(int index) {
        assert index < size();
        return values[offset + index];
    }

    @Override
    public T set(int index, T element) {
        throw new UnsupportedOperationException("modifying list opertations are not implemented");
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (object instanceof SlicedObjectList) {
            SlicedObjectList<?> that = (SlicedObjectList<?>) object;
            int size = size();
            if (that.size() != size) {
                return false;
            }
            for (int i = 0; i < size; i++) {
                if (values[offset + i].equals(that.values[that.offset + i])) {
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
            result = 31 * result + values[offset+i].hashCode();
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

    public abstract void grow(int newLength);
}