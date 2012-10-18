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

package org.elasticsearch.search.facet.terms;

import org.elasticsearch.search.facet.Facet;

import java.util.List;

/**
 * Terms facet allows to return facets of the most popular terms within the search query.
 *
 *
 */
public interface TermsFacet extends Facet, Iterable<TermsFacet.Entry> {

    /**
     * The type of the filter facet.
     */
    public static final String TYPE = "terms";

    public interface Entry extends Comparable<Entry> {

        String term();

        String getTerm();

        Number termAsNumber();

        Number getTermAsNumber();

        int count();

        int getCount();
    }

    /**
     * The number of docs missing a value.
     */
    long missingCount();

    /**
     * The number of docs missing a value.
     */
    long getMissingCount();

    /**
     * The total count of terms.
     */
    long totalCount();

    /**
     * The total count of terms.
     */
    long getTotalCount();

    /**
     * The count of terms other than the one provided by the entries.
     */
    long otherCount();

    /**
     * The count of terms other than the one provided by the entries.
     */
    long getOtherCount();

    /**
     * The terms and counts.
     */
    List<? extends TermsFacet.Entry> entries();

    /**
     * The terms and counts.
     */
    List<? extends TermsFacet.Entry> getEntries();
}