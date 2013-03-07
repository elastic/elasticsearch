package org.apache.lucene.queries;
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
import org.apache.lucene.search.BooleanClause.Occur;
import org.elasticsearch.common.lucene.search.Queries;

/**
 * Extended version of {@link CommonTermsQuery} that allows to pass in a 
 * <tt>minimumNumberShouldMatch</tt> specification that uses the actual num of high frequent terms
 * to calculate the minimum matching terms.
 */
public class ExtendedCommonTermsQuery extends CommonTermsQuery {

    public ExtendedCommonTermsQuery(Occur highFreqOccur, Occur lowFreqOccur, float maxTermFrequency, boolean disableCoord) {
        super(highFreqOccur, lowFreqOccur, maxTermFrequency, disableCoord);
    }

    public ExtendedCommonTermsQuery(Occur highFreqOccur, Occur lowFreqOccur, float maxTermFrequency) {
        super(highFreqOccur, lowFreqOccur, maxTermFrequency);
    }
    
    private String minNumShouldMatchSpec;

    @Override
    protected int calcLowFreqMinimumNumberShouldMatch(int numOptional) {
        if (minNumShouldMatchSpec == null) {
            return 0;
        }
        return Queries.calculateMinShouldMatch(numOptional, minNumShouldMatchSpec);
    }
    
    public void setMinimumNumberShouldMatch(String spec) {
        this.minNumShouldMatchSpec = spec;
    }

}
