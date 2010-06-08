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

package org.elasticsearch.search.facets.histogram;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.search.facets.Facet;

import java.util.Comparator;
import java.util.List;

/**
 * Numeric histogram information.
 *
 * @author kimchy (shay.banon)
 */
public interface HistogramFacet extends Facet, Iterable<HistogramFacet.Entry> {

    String fieldName();

    String getFieldName();

    List<Entry> entries();

    List<Entry> getEntries();

    public static enum ComparatorType {
        VALUE((byte) 0, "value", new Comparator<Entry>() {

            @Override public int compare(Entry o1, Entry o2) {
                // really, there should not be two entries with the same value
                int i = (int) (o1.value() - o2.value());
                if (i == 0) {
                    i = System.identityHashCode(o1) - System.identityHashCode(o2);
                }
                return i;
            }
        }),
        COUNT((byte) 1, "count", new Comparator<Entry>() {

            @Override public int compare(Entry o1, Entry o2) {
                int i = (int) (o2.count() - o1.count());
                if (i == 0) {
                    i = (int) (o2.total() - o1.total());
                    if (i == 0) {
                        i = System.identityHashCode(o2) - System.identityHashCode(o1);
                    }
                }
                return i;
            }
        }),
        TOTAL((byte) 2, "total", new Comparator<Entry>() {

            @Override public int compare(Entry o1, Entry o2) {
                int i = (int) (o2.total() - o1.total());
                if (i == 0) {
                    i = (int) (o2.count() - o1.count());
                    if (i == 0) {
                        i = System.identityHashCode(o2) - System.identityHashCode(o1);
                    }
                }
                return i;
            }
        });

        private final byte id;

        private final String description;

        private final Comparator<Entry> comparator;

        ComparatorType(byte id, String description, Comparator<Entry> comparator) {
            this.id = id;
            this.description = description;
            this.comparator = comparator;
        }

        public byte id() {
            return this.id;
        }

        public String description() {
            return this.description;
        }

        public Comparator<Entry> comparator() {
            return comparator;
        }

        public static ComparatorType fromId(byte id) {
            if (id == 0) {
                return VALUE;
            } else if (id == 1) {
                return COUNT;
            } else if (id == 2) {
                return TOTAL;
            }
            throw new ElasticSearchIllegalArgumentException("No type argument match for histogram comparator [" + id + "]");
        }

        public static ComparatorType fromString(String type) {
            if ("value".equals(type)) {
                return VALUE;
            } else if ("count".equals(type)) {
                return COUNT;
            } else if ("total".equals(type)) {
                return TOTAL;
            }
            throw new ElasticSearchIllegalArgumentException("No type argument match for histogram comparator [" + type + "]");
        }
    }


    public class Entry {
        private final long value;
        private final long count;
        private final double total;

        public Entry(long value, long count, double total) {
            this.value = value;
            this.count = count;
            this.total = total;
        }

        public long value() {
            return value;
        }

        public long getValue() {
            return value();
        }

        public long count() {
            return count;
        }

        public long getCount() {
            return count();
        }

        public double total() {
            return total;
        }

        public double getTotal() {
            return total();
        }

        public double mean() {
            return total / count;
        }

        public double getMean() {
            return mean();
        }
    }
}