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

package org.elasticsearch.search.facet.histogram;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.search.facet.Facet;

import java.util.Comparator;
import java.util.List;

/**
 * Numeric histogram facet.
 */
public interface HistogramFacet extends Facet, Iterable<HistogramFacet.Entry> {

    /**
     * The type of the filter facet.
     */
    public static final String TYPE = "histogram";

    /**
     * An ordered list of histogram facet entries.
     */
    List<? extends Entry> getEntries();

    public static enum ComparatorType {
        KEY((byte) 0, "key", new Comparator<Entry>() {

            @Override
            public int compare(Entry o1, Entry o2) {
                // push nulls to the end
                if (o1 == null) {
                    if (o2 == null) {
                        return 0;
                    }
                    return 1;
                }
                if (o2 == null) {
                    return -1;
                }
                return (o1.getKey() < o2.getKey() ? -1 : (o1.getKey() == o2.getKey() ? 0 : 1));
            }
        }),
        COUNT((byte) 1, "count", new Comparator<Entry>() {

            @Override
            public int compare(Entry o1, Entry o2) {
                // push nulls to the end
                if (o1 == null) {
                    if (o2 == null) {
                        return 0;
                    }
                    return 1;
                }
                if (o2 == null) {
                    return -1;
                }
                return (o1.getCount() < o2.getCount() ? -1 : (o1.getCount() == o2.getCount() ? 0 : 1));
            }
        }),
        TOTAL((byte) 2, "total", new Comparator<Entry>() {

            @Override
            public int compare(Entry o1, Entry o2) {
                // push nulls to the end
                if (o1 == null) {
                    if (o2 == null) {
                        return 0;
                    }
                    return 1;
                }
                if (o2 == null) {
                    return -1;
                }
                return (o1.getTotal() < o2.getTotal() ? -1 : (o1.getTotal() == o2.getTotal() ? 0 : 1));
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
                return KEY;
            } else if (id == 1) {
                return COUNT;
            } else if (id == 2) {
                return TOTAL;
            }
            throw new ElasticsearchIllegalArgumentException("No type argument match for histogram comparator [" + id + "]");
        }

        public static ComparatorType fromString(String type) {
            if ("key".equals(type)) {
                return KEY;
            } else if ("count".equals(type)) {
                return COUNT;
            } else if ("total".equals(type)) {
                return TOTAL;
            }
            throw new ElasticsearchIllegalArgumentException("No type argument match for histogram comparator [" + type + "]");
        }
    }

    public interface Entry {

        /**
         * The key value of the histogram.
         */
        long getKey();

        /**
         * The number of hits that fall within that key "range" or "interval".
         */
        long getCount();

        /**
         * The total count of values aggregated to compute the total.
         */
        long getTotalCount();

        /**
         * The sum / total of the value field that fall within this key "interval".
         */
        double getTotal();

        /**
         * The mean of this facet interval.
         */
        double getMean();

        /**
         * The minimum value.
         */
        double getMin();

        /**
         * The maximum value.
         */
        double getMax();
    }
}