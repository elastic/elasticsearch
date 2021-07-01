/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */


package org.elasticsearch.xpack.ml.aggs.heuristic;


import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.ChiSquaredDistribution;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristicBuilder;

import java.io.IOException;

public class PValueScore extends SignificanceHeuristic {
    public static final String NAME = "p_value";
    public static final ObjectParser<PValueScore, Void> PARSER = new ObjectParser<>(NAME, PValueScore::new);

    public PValueScore() {
    }

    public PValueScore(StreamInput in) {
        // Nothing to read.
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME).endObject();
        return builder;
    }

    public static SignificanceHeuristic parse(XContentParser parser)
            throws IOException, QueryShardException {
        // move to the closing bracket
        if (parser.nextToken().equals(XContentParser.Token.END_OBJECT) == false) {
            throw new ElasticsearchParseException("failed to parse [p_value] significance heuristic. expected an empty object, " +
                    "but got [{}] instead", parser.currentToken());
        }
        return new PValueScore();
    }

    /**
     *  This finds the p-value that the frequency of a category is unchanged on set subset assuming
     *  we observe subsetFreq out of subset values in total relative to set supersetFreq where it accounts
     *  supersetFreq out of supersetSize total.
     *
     *  This assumes that each sample is an independent Bernoulli trial and computes the p-value
     *  under the null hypothesis that the probabilities are the same. Note that the independence
     *  assumption is quite strong and can lead to low p-values even if the fractions are very
     *  similar if there are many trials. We arrange for small differences in frequency to always
     *  have large p-values. We also artificially increase the p-value of when the probability
     *  of the category is very small.
     */
    @Override
    public double getScore(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize) {
        checkFrequencyValidity(subsetFreq, subsetSize, supersetFreq, supersetSize, "PValueScore");
        if (subsetFreq * supersetSize <= subsetSize * supersetFreq) {
            return 1.0;
        }
        if (subsetFreq == 0 || subsetSize == 0 || supersetFreq == 0 || supersetSize == 0) {
            // TODO is this right?
            return 1.0;
        }

        // Adjust counts to ignore ratio changes which are less than 5%
        if (subsetFreq / subsetSize < supersetFreq / supersetSize) {
            subsetFreq = (long)(Math.min(1.05 * subsetFreq, supersetFreq / (double)supersetSize * subsetSize) + 0.5);
        }
        if (subsetFreq / subsetSize > supersetFreq / supersetSize) {
            supersetFreq = (long)(Math.min(1.05 * supersetFreq, subsetFreq / (double)subsetSize * supersetSize) + 0.5);
        }

        long epsSubSetSize = eps(subsetSize);
        long epsSuperSetSize = eps(supersetSize);
        ChiSquaredDistribution chi2 = new ChiSquaredDistribution(1);

        double v1 = new BinomialDistribution(
            (int)(subsetSize + epsSubSetSize),
            (double)(subsetFreq + epsSubSetSize)/(subsetSize + epsSubSetSize)
        ).logProbability((int)(subsetFreq + epsSubSetSize));

        double v2 = new BinomialDistribution(
            (int)(supersetSize + epsSuperSetSize),
            (double)(supersetFreq + epsSuperSetSize)/(supersetSize + epsSuperSetSize)
        ).logProbability((int)(supersetFreq + epsSuperSetSize));

        double p2 = (double)(subsetFreq + supersetFreq + epsSuperSetSize + epsSubSetSize)
            / (subsetSize + supersetSize + epsSuperSetSize + epsSubSetSize);

        double v3 = new BinomialDistribution((int)(subsetSize + epsSubSetSize), p2)
            .logProbability((int)(subsetFreq + epsSubSetSize));

        double v4 = new BinomialDistribution((int)(supersetSize + epsSuperSetSize), p2)
            .logProbability((int)(supersetFreq + epsSuperSetSize));

        double logLikelihoodRatio = v1 + v2 - v3 - v4;
        return 1 - chi2.cumulativeProbability(2.0 * logLikelihoodRatio);
    }

    private long eps(long value) {
        return Math.max((long)(0.05 * (double)value + 0.5), 1);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    public static class PValueScoreBuilder implements SignificanceHeuristicBuilder {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME).endObject();
            return builder;
        }
    }
}

