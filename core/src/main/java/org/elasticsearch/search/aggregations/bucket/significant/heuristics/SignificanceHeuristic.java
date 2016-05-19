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

package org.elasticsearch.search.aggregations.bucket.significant.heuristics;


import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.internal.SearchContext;

public abstract class SignificanceHeuristic implements NamedWriteable, ToXContent {
    /**
     * @param subsetFreq   The frequency of the term in the selected sample
     * @param subsetSize   The size of the selected sample (typically number of docs)
     * @param supersetFreq The frequency of the term in the superset from which the sample was taken
     * @param supersetSize The size of the superset from which the sample was taken  (typically number of docs)
     * @return a "significance" score
     */
    public abstract double getScore(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize);

    protected void checkFrequencyValidity(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize, String scoreFunctionName) {
        if (subsetFreq < 0 || subsetSize < 0 || supersetFreq < 0 || supersetSize < 0) {
            throw new IllegalArgumentException("Frequencies of subset and superset must be positive in " + scoreFunctionName + ".getScore()");
        }
        if (subsetFreq > subsetSize) {
            throw new IllegalArgumentException("subsetFreq > subsetSize, in " + scoreFunctionName);
        }
        if (supersetFreq > supersetSize) {
            throw new IllegalArgumentException("supersetFreq > supersetSize, in " + scoreFunctionName);
        }
    }

    public void initialize(InternalAggregation.ReduceContext reduceContext) {

    }

    public void initialize(SearchContext context) {

    }
}
