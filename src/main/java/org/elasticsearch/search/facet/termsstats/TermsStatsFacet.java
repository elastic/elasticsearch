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

package org.elasticsearch.search.facet.termsstats;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.search.facet.Facet;

import java.util.Comparator;
import java.util.List;

public interface TermsStatsFacet extends Facet, Iterable<TermsStatsFacet.Entry> {

    public static final String TYPE = "terms_stats";


    /**
     * The number of docs missing a value.
     */
    long missingCount();

    /**
     * The number of docs missing a value.
     */
    long getMissingCount();

    /**
     * The terms and counts.
     */
    List<? extends TermsStatsFacet.Entry> entries();

    /**
     * The terms and counts.
     */
    List<? extends TermsStatsFacet.Entry> getEntries();
    
    public abstract class NullToEndComparator implements Comparator<Entry> {
        @Override
        public int compare(Entry o1, Entry o2) {
            if (o1 == null) {
                if (o2 == null) {
                    return 0;
                }
                return 1;
            } else if (o2 == null) {
                return -1;
            }

            return compareByValue(o1, o2);
        }
        
        public abstract int compareByValue(Entry o1, Entry o2);
    }
    
    /**
     * Controls how the terms facets are ordered.
     */
    public static enum ComparatorType {

        /**
         * Order by the (higher) count of each term.
         */
        COUNT((byte) 0, new NullToEndComparator() {
            @Override
            public int compareByValue(Entry o1, Entry o2) {
                return -Long.compare(o1.count(), o2.count());
            }
        }),
        /**
         * Order by the (lower) count of each term.
         */
        REVERSE_COUNT((byte) 1, new NullToEndComparator() {
            @Override
            public int compareByValue(Entry o1, Entry o2) {
                return -COUNT.comparator().compare(o1, o2);
            }
        }),
        /**
         * Order by the terms.
         */
        TERM((byte) 2, new NullToEndComparator() {
            @Override
            public int compareByValue(Entry o1, Entry o2) {
                int result = o1.compareTo(o2);
                return (result == 0) ? COUNT.comparator().compare(o1, o2) : result;
            }
        }),
        /**
         * Order by the terms.
         */
        REVERSE_TERM((byte) 3, new NullToEndComparator() {
            @Override
            public int compareByValue(Entry o1, Entry o2) {
                return -TERM.comparator().compare(o1, o2);
            }
        }),

        TOTAL((byte) 4, new NullToEndComparator() {
            @Override
            public int compareByValue(Entry o1, Entry o2) {
                int result = -Double.compare(o1.total(), o2.total());
                return (result == 0) ? COUNT.comparator().compare(o1, o2) : result;
            }
        }),

        REVERSE_TOTAL((byte) 5, new NullToEndComparator() {
            @Override
            public int compareByValue(Entry o1, Entry o2) {
                return -TOTAL.comparator().compare(o1, o2);
            }
        }),

        MIN((byte) 6, new NullToEndComparator() {
            @Override
            public int compareByValue(Entry o1, Entry o2) {
                int result = Double.compare(o1.min(), o2.min());
                return (result == 0) ? COUNT.comparator().compare(o1, o2) : result;
            }
        }),
        REVERSE_MIN((byte) 7, new NullToEndComparator() {
            @Override
            public int compareByValue(Entry o1, Entry o2) {
                return -MIN.comparator().compare(o1, o2);
            }
        }),
        MAX((byte) 8, new NullToEndComparator() {
            @Override
            public int compareByValue(Entry o1, Entry o2) {
                int result = -Double.compare(o1.max(), o2.max());
                return (result == 0) ? COUNT.comparator().compare(o1, o2) : result;
            }
        }),
        REVERSE_MAX((byte) 9, new NullToEndComparator() {
            @Override
            public int compareByValue(Entry o1, Entry o2) {
                return -MAX.comparator().compare(o1, o2);
            }
        }),;

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
            if (id == COUNT.id()) {
                return COUNT;
            } else if (id == REVERSE_COUNT.id()) {
                return REVERSE_COUNT;
            } else if (id == TERM.id()) {
                return TERM;
            } else if (id == REVERSE_TERM.id()) {
                return REVERSE_TERM;
            } else if (id == TOTAL.id()) {
                return TOTAL;
            } else if (id == REVERSE_TOTAL.id()) {
                return REVERSE_TOTAL;
            } else if (id == MIN.id()) {
                return MIN;
            } else if (id == REVERSE_MIN.id()) {
                return REVERSE_MIN;
            } else if (id == MAX.id()) {
                return MAX;
            } else if (id == REVERSE_MAX.id()) {
                return REVERSE_MAX;
            }
            throw new ElasticSearchIllegalArgumentException("No type argument match for terms facet comparator [" + id + "]");
        }

        public static ComparatorType fromString(String type) {
            if ("count".equals(type)) {
                return COUNT;
            } else if ("term".equals(type)) {
                return TERM;
            } else if ("reverse_count".equals(type) || "reverseCount".equals(type)) {
                return REVERSE_COUNT;
            } else if ("reverse_term".equals(type) || "reverseTerm".equals(type)) {
                return REVERSE_TERM;
            } else if ("total".equals(type)) {
                return TOTAL;
            } else if ("reverse_total".equals(type) || "reverseTotal".equals(type)) {
                return REVERSE_TOTAL;
            } else if ("min".equals(type)) {
                return MIN;
            } else if ("reverse_min".equals(type) || "reverseMin".equals(type)) {
                return REVERSE_MIN;
            } else if ("max".equals(type)) {
                return MAX;
            } else if ("reverse_max".equals(type) || "reverseMax".equals(type)) {
                return REVERSE_MAX;
            }
            throw new ElasticSearchIllegalArgumentException("No type argument match for terms stats facet comparator [" + type + "]");
        }
    }

    public interface Entry extends Comparable<Entry> {

        String term();

        String getTerm();

        Number termAsNumber();

        Number getTermAsNumber();

        long count();

        long getCount();

        long totalCount();

        long getTotalCount();

        double min();

        double getMin();

        double max();

        double getMax();

        double total();

        double getTotal();

        double mean();

        double getMean();
    }
}