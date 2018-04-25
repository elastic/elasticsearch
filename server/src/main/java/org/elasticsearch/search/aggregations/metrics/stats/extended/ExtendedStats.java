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
package org.elasticsearch.search.aggregations.metrics.stats.extended;

import org.elasticsearch.search.aggregations.metrics.stats.Stats;

/**
 * Statistics over a set of values (either aggregated over field data or scripts)
 */
public interface ExtendedStats extends Stats {

    /**
     * The sum of the squares of the collected values.
     */
    double getSumOfSquares();

    /**
     * The variance of the collected values.
     */
    double getVariance();

    /**
     * The standard deviation of the collected values.
     */
    double getStdDeviation();

    /**
     * The upper or lower bounds of the stdDeviation
     */
    double getStdDeviationBound(Bounds bound);

    /**
     * The standard deviation of the collected values as a String.
     */
    String getStdDeviationAsString();

    /**
     * The upper or lower bounds of stdDev of the collected values as a String.
     */
    String getStdDeviationBoundAsString(Bounds bound);


    /**
     * The sum of the squares of the collected values as a String.
     */
    String getSumOfSquaresAsString();

    /**
     * The variance of the collected values as a String.
     */
    String getVarianceAsString();


    enum Bounds {
        UPPER, LOWER
    }

}
