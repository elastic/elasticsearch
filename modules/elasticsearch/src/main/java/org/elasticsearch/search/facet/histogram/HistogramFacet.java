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

package org.elasticsearch.search.facet.histogram;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.search.facet.Facet;

import java.util.Comparator;
import java.util.List;

/**
 * Numeric histogram facet.
 *
 * @author kimchy (shay.banon)
 */
public interface HistogramFacet extends Facet, Iterable<HistogramFacet.Entry> {

    /**
     * The key field name used with this facet.
     */
    String keyFieldName();

    /**
     * The key field name used with this facet.
     */
    String getKeyFieldName();

    /**
     * The value field name used with this facet.
     */
    String valueFieldName();

    /**
     * The value field name used with this facet.
     */
    String getValueFieldName();

    /**
     * An ordered list of histogram facet entries.
     */
    List<Entry> entries();

    /**
     * An ordered list of histogram facet entries.
     */
    List<Entry> getEntries();

    public static enum ComparatorType {
        KEY((byte) 0, "key", new Comparator<Entry>() {

            @Override public int compare(Entry o1, Entry o2) {
                return (o1.key() < o2.key() ? -1 : (o1.key() == o2.key() ? 0 : 1));
            }
        }),
        COUNT((byte) 1, "count", new Comparator<Entry>() {

            @Override public int compare(Entry o1, Entry o2) {
                return (o1.count() < o2.count() ? -1 : (o1.count() == o2.count() ? 0 : 1));
            }
        }),
        TOTAL((byte) 2, "total", new Comparator<Entry>() {

            @Override public int compare(Entry o1, Entry o2) {
                return (o1.total() < o2.total() ? -1 : (o1.total() == o2.total() ? 0 : 1));
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
            throw new ElasticSearchIllegalArgumentException("No type argument match for histogram comparator [" + id + "]");
        }

        public static ComparatorType fromString(String type) {
            if ("key".equals(type)) {
                return KEY;
            } else if ("count".equals(type)) {
                return COUNT;
            } else if ("total".equals(type)) {
                return TOTAL;
            }
            throw new ElasticSearchIllegalArgumentException("No type argument match for histogram comparator [" + type + "]");
        }
    }


    /**
     * A histogram entry representing a single entry within the result of a histogram facet.
     */
    public class Entry {
        private final long key;
        private final long count;
        private final double total;

        public Entry(long key, long count, double total) {
            this.key = key;
            this.count = count;
            this.total = total;
        }

        /**
         * The key value of the histogram.
         */
        public long key() {
            return key;
        }

        /**
         * The key value of the histogram.
         */
        public long getKey() {
            return key();
        }

        /**
         * The number of hits that fall within that key "range" or "interval".
         */
        public long count() {
            return count;
        }

        /**
         * The number of hits that fall within that key "range" or "interval".
         */
        public long getCount() {
            return count();
        }

        /**
         * The sum / total of the value field that fall within this key "interval".
         */
        public double total() {
            return total;
        }

        /**
         * The sum / total of the value field that fall within this key "interval".
         */
        public double getTotal() {
            return total();
        }

        /**
         * The mean of this facet interval.
         */
        public double mean() {
            return total / count;
        }

        /**
         * The mean of this facet interval.
         */
        public double getMean() {
            return mean();
        }
    }
}