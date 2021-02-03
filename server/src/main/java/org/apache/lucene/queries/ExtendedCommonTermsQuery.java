/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.lucene.queries;

import org.apache.lucene.search.BooleanClause.Occur;
import org.elasticsearch.common.lucene.search.Queries;

/**
 * Extended version of {@link CommonTermsQuery} that allows to pass in a
 * {@code minimumNumberShouldMatch} specification that uses the actual num of high frequent terms
 * to calculate the minimum matching terms.
 *
 * @deprecated Since max_optimization optimization landed in 7.0, normal MatchQuery
 *             will achieve the same result without any configuration.
 */
@Deprecated
public class ExtendedCommonTermsQuery extends CommonTermsQuery {

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

    @Override
    protected int calcHighFreqMinimumNumberShouldMatch(int numOptional) {
        return calcMinimumNumberShouldMatch(highFreqMinNumShouldMatchSpec, numOptional);
    }

    public void setHighFreqMinimumNumberShouldMatch(String spec) {
        this.highFreqMinNumShouldMatchSpec = spec;
    }

    public String getHighFreqMinimumNumberShouldMatchSpec() {
        return highFreqMinNumShouldMatchSpec;
    }

    public void setLowFreqMinimumNumberShouldMatch(String spec) {
        this.lowFreqMinNumShouldMatchSpec = spec;
    }

    public String getLowFreqMinimumNumberShouldMatchSpec() {
        return lowFreqMinNumShouldMatchSpec;
    }

    public float getMaxTermFrequency() {
        return this.maxTermFrequency;
    }

}
