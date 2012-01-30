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

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilterClause;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.docset.DocSet;
import org.elasticsearch.common.lucene.docset.DocSets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
// LUCENE MONITOR: added to take into account DocSet that wraps OpenBitSet when optimizing or/and/...
public class XBooleanFilter extends Filter {
    ArrayList<Filter> shouldFilters = null;
    ArrayList<Filter> notFilters = null;
    ArrayList<Filter> mustFilters = null;

    private DocIdSet getDISI(ArrayList<Filter> filters, int index, IndexReader reader)
            throws IOException {
        DocIdSet docIdSet = filters.get(index).getDocIdSet(reader);
        if (docIdSet == DocIdSet.EMPTY_DOCIDSET || docIdSet == DocSet.EMPTY_DOC_SET) {
            return null;
        }
        return docIdSet;
    }

    public List<Filter> getShouldFilters() {
        return this.shouldFilters;
    }

    public List<Filter> getMustFilters() {
        return this.mustFilters;
    }

    public List<Filter> getNotFilters() {
        return this.notFilters;
    }

    /**
     * Returns the a DocIdSetIterator representing the Boolean composition
     * of the filters that have been added.
     */
    @Override
    public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        FixedBitSet res = null;

        if (mustFilters == null && notFilters == null && shouldFilters != null && shouldFilters.size() == 1) {
            return shouldFilters.get(0).getDocIdSet(reader);
        }

        if (shouldFilters == null && notFilters == null && mustFilters != null && mustFilters.size() == 1) {
            return mustFilters.get(0).getDocIdSet(reader);
        }

        if (shouldFilters != null) {
            for (int i = 0; i < shouldFilters.size(); i++) {
                final DocIdSet disi = getDISI(shouldFilters, i, reader);
                if (disi == null) continue;
                if (res == null) {
                    res = new FixedBitSet(reader.maxDoc());
                }
                DocSets.or(res, disi);
            }

            // if no should clauses match, return null (act as min_should_match set to 1)
            if (res == null && !shouldFilters.isEmpty()) {
                return null;
            }
        }


        if (notFilters != null) {
            for (int i = 0; i < notFilters.size(); i++) {
                if (res == null) {
                    res = new FixedBitSet(reader.maxDoc());
                    res.set(0, reader.maxDoc()); // NOTE: may set bits on deleted docs
                }
                final DocIdSet disi = getDISI(notFilters, i, reader);
                if (disi != null) {
                    DocSets.andNot(res, disi);
                }
            }
        }

        if (mustFilters != null) {
            for (int i = 0; i < mustFilters.size(); i++) {
                final DocIdSet disi = getDISI(mustFilters, i, reader);
                if (disi == null) {
                    return null;
                }
                if (res == null) {
                    res = new FixedBitSet(reader.maxDoc());
                    DocSets.or(res, disi);
                } else {
                    DocSets.and(res, disi);
                }
            }
        }

        return res;
    }

    /**
     * Adds a new FilterClause to the Boolean Filter container
     *
     * @param filterClause A FilterClause object containing a Filter and an Occur parameter
     */

    public void add(FilterClause filterClause) {
        if (filterClause.getOccur().equals(BooleanClause.Occur.MUST)) {
            if (mustFilters == null) {
                mustFilters = new ArrayList<Filter>();
            }
            mustFilters.add(filterClause.getFilter());
        }
        if (filterClause.getOccur().equals(BooleanClause.Occur.SHOULD)) {
            if (shouldFilters == null) {
                shouldFilters = new ArrayList<Filter>();
            }
            shouldFilters.add(filterClause.getFilter());
        }
        if (filterClause.getOccur().equals(BooleanClause.Occur.MUST_NOT)) {
            if (notFilters == null) {
                notFilters = new ArrayList<Filter>();
            }
            notFilters.add(filterClause.getFilter());
        }
    }

    public void addMust(Filter filter) {
        if (mustFilters == null) {
            mustFilters = new ArrayList<Filter>();
        }
        mustFilters.add(filter);
    }

    public void addShould(Filter filter) {
        if (shouldFilters == null) {
            shouldFilters = new ArrayList<Filter>();
        }
        shouldFilters.add(filter);
    }

    public void addNot(Filter filter) {
        if (notFilters == null) {
            notFilters = new ArrayList<Filter>();
        }
        notFilters.add(filter);
    }


    private boolean equalFilters(ArrayList<Filter> filters1, ArrayList<Filter> filters2) {
        return (filters1 == filters2) ||
                ((filters1 != null) && filters1.equals(filters2));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if ((obj == null) || (obj.getClass() != this.getClass()))
            return false;

        XBooleanFilter other = (XBooleanFilter) obj;
        return equalFilters(notFilters, other.notFilters)
                && equalFilters(mustFilters, other.mustFilters)
                && equalFilters(shouldFilters, other.shouldFilters);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (null == mustFilters ? 0 : mustFilters.hashCode());
        hash = 31 * hash + (null == notFilters ? 0 : notFilters.hashCode());
        hash = 31 * hash + (null == shouldFilters ? 0 : shouldFilters.hashCode());
        return hash;
    }

    /**
     * Prints a user-readable version of this query.
     */
    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("BooleanFilter(");
        appendFilters(shouldFilters, "", buffer);
        appendFilters(mustFilters, "+", buffer);
        appendFilters(notFilters, "-", buffer);
        buffer.append(")");
        return buffer.toString();
    }

    private void appendFilters(ArrayList<Filter> filters, String occurString, StringBuilder buffer) {
        if (filters != null) {
            for (int i = 0; i < filters.size(); i++) {
                buffer.append(' ');
                buffer.append(occurString);
                buffer.append(filters.get(i).toString());
            }
        }
    }
}
