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

package org.elasticsearch.search.aggregations.reducers.movavg.models;

import com.google.common.collect.EvictingQueue;

import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

public abstract class MovAvgModel {

    /**
     * Returns the next value in the series, according to the underlying smoothing model
     *
     * @param values    Collection of numerics to movingAvg, usually windowed
     * @param <T>       Type of numeric
     * @return          Returns a double, since most smoothing methods operate on floating points
     */
    public abstract <T extends Number> double next(Collection<T> values);

    /**
     * Predicts the next `n` values in the series, using the smoothing model to generate new values.
     * Default prediction mode is to simply continuing calling <code>next()</code> and adding the
     * predicted value back into the windowed buffer.
     *
     * @param values            Collection of numerics to movingAvg, usually windowed
     * @param numPredictions    Number of newly generated predictions to return
     * @param <T>               Type of numeric
     * @return                  Returns an array of doubles, since most smoothing methods operate on floating points
     */
    public <T extends Number> double[] predict(Collection<T> values, int numPredictions) {
        double[] predictions = new double[numPredictions];

        // If there are no values, we can't do anything.  Return an array of NaNs.
        if (values.size() == 0) {
            return emptyPredictions(numPredictions);
        }

        // special case for one prediction, avoids allocation
        if (numPredictions < 1) {
            throw new IllegalArgumentException("numPredictions may not be less than 1.");
        } else if (numPredictions == 1){
            predictions[0] = next(values);
            return predictions;
        }

        Collection<Number> predictionBuffer = EvictingQueue.create(values.size());
        predictionBuffer.addAll(values);

        for (int i = 0; i < numPredictions; i++) {
            predictions[i] = next(predictionBuffer);

            // Add the last value to the buffer, so we can keep predicting
            predictionBuffer.add(predictions[i]);
        }

        return predictions;
    }

    protected double[] emptyPredictions(int numPredictions) {
        double[] predictions = new double[numPredictions];
        Arrays.fill(predictions, Double.NaN);
        return predictions;
    }

    /**
     * Write the model to the output stream
     *
     * @param out   Output stream
     * @throws IOException
     */
    public abstract void writeTo(StreamOutput out) throws IOException;
}




