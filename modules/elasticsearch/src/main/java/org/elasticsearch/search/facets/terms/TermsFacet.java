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

package org.elasticsearch.search.facets.terms;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.search.facets.Facet;

import java.util.Comparator;
import java.util.List;

/**
 * @author kimchy (Shay Banon)
 */
public interface TermsFacet extends Facet, Iterable<TermsFacet.Entry> {

    public static enum ComparatorType {
        COUNT((byte) 0, new Comparator<Entry>() {

            @Override public int compare(Entry o1, Entry o2) {
                int i = o2.count() - o1.count();
                if (i == 0) {
                    i = o2.term().compareTo(o1.term());
                    if (i == 0) {
                        i = System.identityHashCode(o2) - System.identityHashCode(o1);
                    }
                }
                return i;
            }
        }),
        TERM((byte) 1, new Comparator<Entry>() {

            @Override public int compare(Entry o1, Entry o2) {
                int i = o2.term().compareTo(o1.term());
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
                return TERM;
            }
            throw new ElasticSearchIllegalArgumentException("No type argument match for multi count comparator [" + id + "]");
        }
    }

    public class Entry {

        private String term;
        private int count;

        public Entry(String term, int count) {
            this.term = term;
            this.count = count;
        }

        public String term() {
            return term;
        }

        public String getTerm() {
            return term;
        }

        public int count() {
            return count;
        }

        public int getCount() {
            return count();
        }
    }

    List<Entry> entries();

    List<Entry> getEntries();
}