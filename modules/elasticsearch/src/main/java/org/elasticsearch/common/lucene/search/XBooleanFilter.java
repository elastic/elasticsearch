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

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilterClause;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.OpenBitSetDISI;
import org.elasticsearch.common.lucene.docset.DocSets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author kimchy (shay.banon)
 */
// LUCENE MONITOR: added to take into account DocSet that wraps OpenBitSet when optimizing or/and/...
public class XBooleanFilter extends Filter {
    ArrayList<Filter> shouldFilters = null;
    ArrayList<Filter> notFilters = null;
    ArrayList<Filter> mustFilters = null;

    private DocIdSetIterator getDISI(ArrayList<Filter> filters, int index, IndexReader reader)
            throws IOException {
        DocIdSet docIdSet = filters.get(index).getDocIdSet(reader);
        if (docIdSet == null) {
            return DocIdSet.EMPTY_DOCIDSET.iterator();
        }
        DocIdSetIterator iterator = docIdSet.iterator();
        if (iterator == null) {
            return DocIdSet.EMPTY_DOCIDSET.iterator();
        }
        return iterator;
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

        if (shouldFilters != null) {
            for (int i = 0; i < shouldFilters.size(); i++) {
                if (res == null) {
                    res = DocSets.createFixedBitSet(getDISI(shouldFilters, i, reader), reader.maxDoc());
                } else {
                    DocIdSet dis = shouldFilters.get(i).getDocIdSet(reader);
                    DocSets.or(res, dis);
                }
            }
        }

        if (notFilters != null) {
            for (int i = 0; i < notFilters.size(); i++) {
                if (res == null) {
                    res = DocSets.createFixedBitSet(getDISI(notFilters, i, reader), reader.maxDoc());
                    res.flip(0, reader.maxDoc()); // NOTE: may set bits on deleted docs
                } else {
                    DocIdSet dis = notFilters.get(i).getDocIdSet(reader);
                    DocSets.andNot(res, dis);
                }
            }
        }

        if (mustFilters != null) {
            for (int i = 0; i < mustFilters.size(); i++) {
                if (res == null) {
                    res = DocSets.createFixedBitSet(getDISI(mustFilters, i, reader), reader.maxDoc());
                } else {
                    DocIdSet dis = mustFilters.get(i).getDocIdSet(reader);
                    DocSets.and(res, dis);
                }
            }
        }

        return res;
    }

    /**
     * Provide a SortedVIntList when it is definitely smaller
     * than an OpenBitSet.
     *
     * @deprecated Either use CachingWrapperFilter, or
     *             switch to a different DocIdSet implementation yourself.
     *             This method will be removed in Lucene 4.0
     */
    protected final DocIdSet finalResult(OpenBitSetDISI result, int maxDocs) {
        return result;
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
