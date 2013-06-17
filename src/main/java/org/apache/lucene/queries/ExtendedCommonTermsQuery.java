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
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

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
    
    private String lowFreqMinNumShouldMatchSpec;
    private String highFreqMinNumShouldMatchSpec;

    @Override
    protected int calcLowFreqMinimumNumberShouldMatch(int numOptional) {
        return calcMinimumNumberShouldMatch(lowFreqMinNumShouldMatchSpec, numOptional);
    }

    protected int calcMinimumNumberShouldMatch(String spec, int numOptional) {
        if (spec == null) {
            return 0;
        }
        return Queries.calculateMinShouldMatch(numOptional, spec);
    }

    protected int calcHighFreqMinimumNumberShouldMatch(int numOptional) {
        return calcMinimumNumberShouldMatch(highFreqMinNumShouldMatchSpec, numOptional);
    }
 
    public void setHighFreqMinimumNumberShouldMatch(String spec) {
        this.highFreqMinNumShouldMatchSpec = spec;
    }

    public String getHighFreqMinimumNumberShouldMatch() {
        return highFreqMinNumShouldMatchSpec;
    }

    public void setLowFreqMinimumNumberShouldMatch(String spec) {
        this.lowFreqMinNumShouldMatchSpec = spec;
    }

    public String getLowFreqMinimumNumberShouldMatch() {
        return lowFreqMinNumShouldMatchSpec;
    }

    @Override
    protected Query buildQuery(final int maxDoc, final TermContext[] contextArray, final Term[] queryTerms) {
        BooleanQuery lowFreq = new BooleanQuery(disableCoord);
        BooleanQuery highFreq = new BooleanQuery(disableCoord);
        highFreq.setBoost(highFreqBoost);
        lowFreq.setBoost(lowFreqBoost);
        BooleanQuery query = new BooleanQuery(true);

        for (int i = 0; i < queryTerms.length; i++) {
            TermContext termContext = contextArray[i];
            if (termContext == null) {
                lowFreq.add(new TermQuery(queryTerms[i]), lowFreqOccur);
            } else {
                if ((maxTermFrequency >= 1f && termContext.docFreq() > maxTermFrequency) || (termContext.docFreq() > (int) Math.ceil(maxTermFrequency * (float) maxDoc))) {
                    highFreq.add(new TermQuery(queryTerms[i], termContext), highFreqOccur);
                } else {
                    lowFreq.add(new TermQuery(queryTerms[i], termContext), lowFreqOccur);
                }
            }
        }

        final int numLowFreqClauses = lowFreq.clauses().size(),
                  numHighFreqClauses = highFreq.clauses().size();

        if (lowFreqOccur == Occur.SHOULD && numLowFreqClauses > 0) {
            int minMustMatch = calcLowFreqMinimumNumberShouldMatch(numLowFreqClauses);
            lowFreq.setMinimumNumberShouldMatch(minMustMatch);
        }

        if (highFreqOccur == Occur.SHOULD && numHighFreqClauses > 0) {
            int minMustMatch = calcHighFreqMinimumNumberShouldMatch(numHighFreqClauses);
            highFreq.setMinimumNumberShouldMatch(minMustMatch);
        }

        if (lowFreq.clauses().isEmpty()) {
            /*
             * if lowFreq is empty we rewrite the high freq terms in a conjunction to
             * prevent slow queries. 
             * Only if a specic high_freq should_match is not specified.
             */
            if (highFreqMinNumShouldMatchSpec == null && highFreqOccur != Occur.MUST) {
                for (BooleanClause booleanClause : highFreq) {
                    booleanClause.setOccur(Occur.MUST);
                }
            }
            highFreq.setBoost(getBoost());
            return highFreq;
        } else if (highFreq.clauses().isEmpty()) {
            // only do low freq terms - we don't have high freq terms
            lowFreq.setBoost(getBoost());
            return lowFreq;
        } else {
            query.add(highFreq, Occur.SHOULD);
            query.add(lowFreq, Occur.MUST);
            query.setBoost(getBoost());
            return query;
        }
    }
}
