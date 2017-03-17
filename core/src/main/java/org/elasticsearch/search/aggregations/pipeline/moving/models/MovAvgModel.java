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

package org.elasticsearch.search.aggregations.pipeline.moving.models;

import java.util.Arrays;
import java.util.Collection;

public abstract class MovAvgModel extends MovModel {

    /**
     * Should this model be fit to the data via a cost minimizing algorithm by default?
     */
    public boolean minimizeByDefault() {
        return false;
    }

    /**
     * Returns if the model can be cost minimized.  Not all models have parameters
     * which can be tuned / optimized.
     */
    public abstract boolean canBeMinimized();

    /**
     * Generates a "neighboring" model, where one of the tunable parameters has been
     * randomly mutated within the allowed range.  Used for minimization
     */
    public abstract MovAvgModel neighboringModel();



    /**
     * Predicts the next `n` values in the series.
     *
     * @param values            Collection of numerics to movingAvg, usually windowed
     * @param numPredictions    Number of newly generated predictions to return
     * @return                  Returns an array of doubles, since most smoothing methods operate on floating points
     */
    public double[] predict(Collection<Double> values, int numPredictions) {
        assert(numPredictions >= 1);

        // If there are no values, we can't do anything.  Return an array of NaNs.
        if (values.isEmpty()) {
            return emptyPredictions(numPredictions);
        }

        return doPredict(values, numPredictions);
    }

    /**
     * Calls to the model-specific implementation which actually generates the predictions
     *
     * @param values            Collection of numerics to movingAvg, usually windowed
     * @param numPredictions    Number of newly generated predictions to return
     * @return                  Returns an array of doubles, since most smoothing methods operate on floating points
     */
    protected abstract double[] doPredict(Collection<Double> values, int numPredictions);

    /**
     * Returns an empty set of predictions, filled with NaNs
     * @param numPredictions Number of empty predictions to generate
     */
    double[] emptyPredictions(int numPredictions) {
        double[] predictions = new double[numPredictions];
        Arrays.fill(predictions, Double.NaN);
        return predictions;
    }


}




