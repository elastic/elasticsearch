/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.collect.EvictingQueue;

/**
 * A cost minimizer which will fit a MovAvgModel to the data.
 *
 * This optimizer uses naive simulated annealing.  Random solutions in the problem space
 * are generated, compared against the last period of data, and the least absolute deviation
 * is recorded as a cost.
 *
 * If the new cost is better than the old cost, the new coefficients are chosen.  If the new
 * solution is worse, there is a temperature-dependent probability it will be randomly selected
 * anyway.  This allows the algo to sample the problem space widely.  As iterations progress,
 * the temperature decreases and the algorithm rejects poor solutions more regularly,
 * theoretically honing in on a global minimum.
 */
public class SimulatedAnealingMinimizer {

    /**
     * Runs the simulated annealing algorithm and produces a model with new coefficients that, theoretically
     * fit the data better and generalizes to future forecasts without overfitting.
     *
     * @param model         The MovAvgModel to be optimized for
     * @param train         A training set provided to the model, which predictions will be
     *                      generated from
     * @param test          A test set of data to compare the predictions against and derive
     *                      a cost for the model
     * @return              A new, minimized model that (theoretically) better fits the data
     */
    public static MovAvgModel minimize(MovAvgModel model, EvictingQueue<Double> train, double[] test) {

        double temp = 1;
        double minTemp = 0.0001;
        int iterations = 100;
        double alpha = 0.9;

        MovAvgModel bestModel = model;
        MovAvgModel oldModel = model;

        double oldCost = cost(model, train, test);
        double bestCost = oldCost;

        while (temp > minTemp) {
            for (int i = 0; i < iterations; i++) {
                MovAvgModel newModel = oldModel.neighboringModel();
                double newCost = cost(newModel, train, test);

                double ap = acceptanceProbability(oldCost, newCost, temp);
                if (ap > Math.random()) {
                    oldModel = newModel;
                    oldCost = newCost;

                    if (newCost < bestCost) {
                        bestCost = newCost;
                        bestModel = newModel;
                    }
                }
            }

            temp *= alpha;
        }

        return bestModel;
    }

    /**
     * If the new cost is better than old, return 1.0.  Otherwise, return a double that increases
     * as the two costs are closer to each other.
     *
     * @param oldCost   Old model cost
     * @param newCost   New model cost
     * @param temp      Current annealing temperature
     * @return          The probability of accepting the new cost over the old
     */
    private static double acceptanceProbability(double oldCost, double newCost, double temp) {
        return newCost < oldCost ? 1.0 : Math.exp(-(newCost - oldCost) / temp);
    }

    /**
     * Calculates the "cost" of a model.  E.g. when run on the training data, how closely do the  predictions
     * match the test data
     *
     * Uses Least Absolute Differences to calculate error.  Note that this is not scale free, but seems
     * to work fairly well in practice
     *
     * @param model     The MovAvgModel we are fitting
     * @param train     A training set of data given to the model, which will then generate predictions from
     * @param test      A test set of data to compare against the predictions
     * @return          A cost, or error, of the model
     */
    private static double cost(MovAvgModel model, EvictingQueue<Double> train, double[] test) {
        double error = 0;
        double[] predictions = model.predict(train, test.length);

        assert (predictions.length == test.length);

        for (int i = 0; i < predictions.length; i++) {
            error += Math.abs(test[i] - predictions[i]);
        }

        return error;
    }

}
