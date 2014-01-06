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

package org.elasticsearch.search.facet.terms;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.facet.Facet;

import java.util.Comparator;
import java.util.List;

/**
 * Terms facet allows to return facets of the most popular terms within the search query.
 */
public interface TermsFacet extends Facet, Iterable<TermsFacet.Entry> {

    /**
     * The type of the filter facet.
     */
    public static final String TYPE = "terms";

    public interface Entry extends Comparable<Entry> {

        Text getTerm();

        Number getTermAsNumber();

        int getCount();
    }

    /**
     * Controls how the terms facets are ordered.
     */
    public static enum ComparatorType {
        /**
         * Order by the (higher) count of each term.
         */
        COUNT((byte) 0, new Comparator<Entry>() {

            @Override
            public int compare(Entry o1, Entry o2) {
                int i = o2.getCount() - o1.getCount();
                if (i == 0) {
                    i = o2.compareTo(o1);
                    if (i == 0) {
                        i = System.identityHashCode(o2) - System.identityHashCode(o1);
                    }
                }
                return i;
            }
        }),
        /**
         * Order by the (lower) count of each term.
         */
        REVERSE_COUNT((byte) 1, new Comparator<Entry>() {

            @Override
            public int compare(Entry o1, Entry o2) {
                return -COUNT.comparator().compare(o1, o2);
            }
        }),
        /**
         * Order by the terms.
         */
        TERM((byte) 2, new Comparator<Entry>() {

            @Override
            public int compare(Entry o1, Entry o2) {
                return o1.compareTo(o2);
            }
        }),
        /**
         * Order by the terms.
         */
        REVERSE_TERM((byte) 3, new Comparator<Entry>() {

            @Override
            public int compare(Entry o1, Entry o2) {
                return -TERM.comparator().compare(o1, o2);
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
            if (id == COUNT.id()) {
                return COUNT;
            } else if (id == REVERSE_COUNT.id()) {
                return REVERSE_COUNT;
            } else if (id == TERM.id()) {
                return TERM;
            } else if (id == REVERSE_TERM.id()) {
                return REVERSE_TERM;
            }
            throw new ElasticsearchIllegalArgumentException("No type argument match for terms facet comparator [" + id + "]");
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
            }
            throw new ElasticsearchIllegalArgumentException("No type argument match for terms facet comparator [" + type + "]");
        }
    }

    /**
     * The number of docs missing a value.
     */
    long getMissingCount();

    /**
     * The total count of terms.
     */
    long getTotalCount();

    /**
     * The count of terms other than the one provided by the entries.
     */
    long getOtherCount();

    /**
     * The terms and counts.
     */
    List<? extends TermsFacet.Entry> getEntries();
}