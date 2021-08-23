/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */


package org.elasticsearch.xpack.ml.aggs.heuristic;


import org.apache.commons.math3.util.FastMath;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.NXYSignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class PValueScore extends NXYSignificanceHeuristic {
    public static final String NAME = "p_value";
    public static final ConstructingObjectParser<PValueScore, Void> PARSER = new ConstructingObjectParser<>(NAME, args -> {
        boolean backgroundIsSuperset = args[0] == null || (boolean) args[0];
        return new PValueScore(backgroundIsSuperset);
    });
    static {
        PARSER.declareBoolean(optionalConstructorArg(), BACKGROUND_IS_SUPERSET);
    }

    private static final MlChiSquaredDistribution CHI_SQUARED_DISTRIBUTION = new MlChiSquaredDistribution(1);

    public PValueScore(boolean backgroundIsSuperset) {
        super(true, backgroundIsSuperset);
    }

    public PValueScore(StreamInput in) throws IOException {
        super(true, in.readBoolean());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(backgroundIsSuperset);
    }

    @Override
    public boolean equals(Object obj) {
        if ((obj instanceof PValueScore) == false) {
            return false;
        }
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        int result = NAME.hashCode();
        result = 31 * result + super.hashCode();
        return result;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(BACKGROUND_IS_SUPERSET.getPreferredName(), backgroundIsSuperset);
        builder.endObject();
        return builder;
    }

    public static SignificanceHeuristic parse(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
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
     *
     *  NOTE: Since in the original calculation of `p-value`, smaller indicates more significance, the value actual value returned
     *        is `log(-p-value)`. To get the original p-value from the score, simply calculate `exp(-retval)`
     *
     * @return log(-p-value)
     */
    @Override
    public double getScore(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize) {
        Frequencies frequencies = computeNxys(subsetFreq, subsetSize, supersetFreq, supersetSize, "PValueScore");
        double docsContainTermInClass = frequencies.N11;
        double allDocsInClass = frequencies.N_1;
        double docsContainTermNotInClass = frequencies.N10;
        double allDocsNotInClass = frequencies.N_0;

        if (docsContainTermInClass * allDocsNotInClass <= allDocsInClass * docsContainTermNotInClass) {
            return 0.0;
        }

        if (allDocsNotInClass == 0L || allDocsInClass == 0L) {
            return 0.0;
        }

        // casting to `long` to round down to nearest whole number
        double epsAllDocsInClass = (long)eps(allDocsInClass);
        double epsAllDocsNotInClass = (long)eps(allDocsNotInClass);

        docsContainTermInClass += epsAllDocsInClass;
        docsContainTermNotInClass += epsAllDocsNotInClass;
        allDocsInClass += epsAllDocsInClass;
        allDocsNotInClass += epsAllDocsNotInClass;

        // Adjust counts to ignore ratio changes which are less than 5%
        // casting to `long` to round down to nearest whole number
        docsContainTermNotInClass = (long)(Math.min(
            1.05 * docsContainTermNotInClass,
            docsContainTermInClass / allDocsInClass * allDocsNotInClass
        ) + 0.5);

        if (allDocsInClass > Long.MAX_VALUE
            || docsContainTermInClass > Long.MAX_VALUE
            || allDocsNotInClass > Long.MAX_VALUE
            || docsContainTermNotInClass > Long.MAX_VALUE) {
            throw new AggregationExecutionException(
                "too many documents in background and foreground sets, further restrict sets for execution"
            );
        }

        double v1 = new LongBinomialDistribution(
            (long)allDocsInClass, docsContainTermInClass / allDocsInClass
        ).logProbability((long)docsContainTermInClass);

        double v2 = new LongBinomialDistribution(
            (long)allDocsNotInClass, docsContainTermNotInClass / allDocsNotInClass
        ).logProbability((long)docsContainTermNotInClass);

        double p2 = (docsContainTermInClass + docsContainTermNotInClass) / (allDocsInClass + allDocsNotInClass);

        double v3 = new LongBinomialDistribution((long)allDocsInClass, p2)
            .logProbability((long)docsContainTermInClass);

        double v4 = new LongBinomialDistribution((long)allDocsNotInClass, p2)
            .logProbability((long)docsContainTermNotInClass);

        double logLikelihoodRatio = v1 + v2 - v3 - v4;
        double pValue = CHI_SQUARED_DISTRIBUTION.survivalFunction(2.0 * logLikelihoodRatio);
        return FastMath.max(-FastMath.log(FastMath.max(pValue, Double.MIN_NORMAL)), 0.0);
    }

    private double eps(double value) {
        return Math.max(0.05 * value + 0.5, 1.0);
    }

    public static class PValueScoreBuilder extends NXYBuilder {

        public PValueScoreBuilder(boolean backgroundIsSuperset) {
            super(true, backgroundIsSuperset);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.field(BACKGROUND_IS_SUPERSET.getPreferredName(), backgroundIsSuperset);
            builder.endObject();
            return builder;
        }
    }
}

