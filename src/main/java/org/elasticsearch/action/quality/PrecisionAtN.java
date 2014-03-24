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

package org.elasticsearch.action.quality;

import org.elasticsearch.search.SearchHit;

import java.util.Set;

import javax.naming.directory.SearchResult;

/**
 * Evaluate Precision at N, N being the number of search results to consider for precision calculation.
 * */
public class PrecisionAtN implements RankedListQualityMetric {
    
    /** Number of results to check against a given set of relevant results. */
    private final int n;
    
    /**
     * @param n number of top results to check against a given set of relevant results.
     * */
    public PrecisionAtN(int n) {
        this.n= n;
    }

    /** Compute precisionAtN based on provided relevant document IDs.
     * @param relevantDocIds set of docIds considered relevant for some query.
     * @param hits hits as returned for some query
     * @return precision at n for above {@link SearchResult} list.
     **/
    public double evaluate(Set<String> relevantDocIds, SearchHit[] hits) {
        int good = 0;
        for (int i = 0; (i < 5 && i < hits.length); i++) {
            if (relevantDocIds.contains(hits[i].getId())) {
                good++;
            }
        }

        if (hits.length < n)
            return ((double) good) / hits.length;
        return ((double) good) / n;
    }
}
