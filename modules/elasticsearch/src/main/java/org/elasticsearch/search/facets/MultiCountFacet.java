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

package org.elasticsearch.search.facets;

import org.elasticsearch.ElasticSearchIllegalArgumentException;

import java.util.Comparator;
import java.util.List;

/**
 * @author kimchy (Shay Banon)
 */
public interface MultiCountFacet<T extends Comparable> extends Facet, Iterable<MultiCountFacet.Entry<T>> {

    public static enum ComparatorType {
        COUNT((byte) 0, new Comparator<Entry>() {

            @Override public int compare(Entry o1, Entry o2) {
                int i = o2.count() - o1.count();
                if (i == 0) {
                    i = o2.value().compareTo(o1.value());
                    if (i == 0) {
                        i = System.identityHashCode(o2) - System.identityHashCode(o1);
                    }
                }
                return i;
            }
        }),
        VALUE((byte) 1, new Comparator<Entry>() {

            @Override public int compare(Entry o1, Entry o2) {
                int i = o2.value().compareTo(o1.value());
                if (i == 0) {
                    i = o2.count() - o1.count();
                    if (i == 0) {
                        i = System.identityHashCode(o2) - System.identityHashCode(o1);
                    }
                }
                return i;
            }
        });

        private final byte id;

        private final Comparator<Entry> comparator;

        ComparatorType(byte id, Comparator<Entry> comparator) {
            this.id = id;
            this.comparator = comparator;
        }

        public byte id() {
            return this.id;
        }

        public Comparator<Entry> comparator() {
            return comparator;
        }

        public static ComparatorType fromId(byte id) {
            if (id == 0) {
                return COUNT;
            } else if (id == 1) {
                return VALUE;
            }
            throw new ElasticSearchIllegalArgumentException("No type argument match for multi count comparator [" + id + "]");
        }
    }

    public static enum ValueType {
        STRING((byte) 0),
        SHORT((byte) 1),
        INT((byte) 2),
        LONG((byte) 3),
        FLOAT((byte) 4),
        DOUBLE((byte) 5);

        private final byte id;

        ValueType(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static ValueType fromId(byte id) {
            if (id == 0) {
                return STRING;
            } else if (id == 1) {
                return SHORT;
            } else if (id == 2) {
                return INT;
            } else if (id == 3) {
                return LONG;
            } else if (id == 4) {
                return FLOAT;
            } else if (id == 5) {
                return DOUBLE;
            }
            throw new ElasticSearchIllegalArgumentException("No type argument match for multi count facet [" + id + "]");
        }
    }

    public class Entry<T extends Comparable> {

        private T value;
        private int count;

        public Entry(T value, int count) {
            this.value = value;
            this.count = count;
        }

        public T value() {
            return value;
        }

        public T getValue() {
            return value;
        }

        public String valueAsString() {
            return value.toString();
        }

        public String getValueAsString() {
            return valueAsString();
        }

        public Number valueAsNumber() {
            if (value instanceof Number) {
                return (Number) value;
            }
            return null;
        }

        public Number getValueAsNumber() {
            return valueAsNumber();
        }

        public int count() {
            return count;
        }

        public int getCount() {
            return count();
        }
    }


    ValueType valueType();

    ValueType getValueType();

    List<Entry<T>> entries();

    List<Entry<T>> getEntries();
}
