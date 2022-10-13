/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

/**
 * Statistics over a set of values (either aggregated over field data or scripts)
 */
public interface ExtendedStats extends Stats {

    /**
     * The sum of the squares of the collected values.
     */
    double getSumOfSquares();

    /**
     * The population variance of the collected values.
     */
    double getVariance();

    /**
     * The population variance of the collected values.
     */
    double getVariancePopulation();

    /**
     * The sampling variance of the collected values.
     */
    double getVarianceSampling();

    /**
     * The population standard deviation of the collected values.
     */
    double getStdDeviation();

    /**
     * The population standard deviation of the collected values.
     */
    double getStdDeviationPopulation();

    /**
     * The sampling standard deviation of the collected values.
     */
    double getStdDeviationSampling();

    /**
     * The upper or lower bounds of the stdDeviation
     */
    double getStdDeviationBound(Bounds bound);

    /**
     * The population standard deviation of the collected values as a String.
     */
    String getStdDeviationAsString();

    /**
     * The population standard deviation of the collected values as a String.
     */
    String getStdDeviationPopulationAsString();

    /**
     * The sampling standard deviation of the collected values as a String.
     */
    String getStdDeviationSamplingAsString();

    /**
     * The upper or lower bounds of stdDev of the collected values as a String.
     */
    String getStdDeviationBoundAsString(Bounds bound);

    /**
     * The sum of the squares of the collected values as a String.
     */
    String getSumOfSquaresAsString();

    /**
     * The population variance of the collected values as a String.
     */
    String getVarianceAsString();

    /**
     * The population variance of the collected values as a String.
     */
    String getVariancePopulationAsString();

    /**
     * The sampling variance of the collected values as a String.
     */
    String getVarianceSamplingAsString();

    enum Bounds {
        UPPER,
        LOWER,
        UPPER_POPULATION,
        LOWER_POPULATION,
        UPPER_SAMPLING,
        LOWER_SAMPLING
    }

}
