/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.dataframe.process.crossvalidation;

import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractor;

import java.util.List;
import java.util.Random;

/**
 * This is a streaming implementation of a cross validation splitter that
 * is based on the reservoir idea. It randomly picks training docs while
 * respecting the exact training percent.
 */
abstract class AbstractReservoirCrossValidationSplitter implements CrossValidationSplitter {

    protected final int dependentVariableIndex;
    private final double samplingRatio;
    private final Random random;

    AbstractReservoirCrossValidationSplitter(List<String> fieldNames, String dependentVariable, double trainingPercent,
                                             long randomizeSeed) {
        assert trainingPercent >= 1.0 && trainingPercent <= 100.0;
        this.dependentVariableIndex = findDependentVariableIndex(fieldNames, dependentVariable);
        this.samplingRatio = trainingPercent / 100.0;
        this.random = new Random(randomizeSeed);
    }

    private static int findDependentVariableIndex(List<String> fieldNames, String dependentVariable) {
        int dependentVariableIndex = fieldNames.indexOf(dependentVariable);
        if (dependentVariableIndex < 0) {
            throw ExceptionsHelper.serverError("Could not find dependent variable [" + dependentVariable + "] in fields " + fieldNames);
        }
        return dependentVariableIndex;
    }

    @Override
    public void process(String[] row, Runnable incrementTrainingDocs, Runnable incrementTestDocs) {

        if (canBeUsedForTraining(row) == false) {
            incrementTestDocs.run();
            return;
        }

        SampleInfo sample = getSampleInfo(row);

        // We ensure the target sample count is at least 1 as if the class count
        // is too low we might get a target of zero and, thus, no samples of the whole class
        long targetSampleCount = (long) Math.max(1.0, samplingRatio * sample.classCount);

        // The idea here is that the probability increases as the chances we have to get the target proportion
        // for a class decreases.
        double p = (double) (targetSampleCount - sample.training) / (sample.classCount - sample.observed);

        boolean isTraining = random.nextDouble() <= p;

        sample.observed++;
        if (isTraining) {
            sample.training++;
            incrementTrainingDocs.run();
        } else {
            row[dependentVariableIndex] = DataFrameDataExtractor.NULL_VALUE;
            incrementTestDocs.run();
        }
    }

    private boolean canBeUsedForTraining(String[] row) {
        return row[dependentVariableIndex] != DataFrameDataExtractor.NULL_VALUE;
    }

    protected abstract SampleInfo getSampleInfo(String[] row);

    /**
     * Class count, count of docs picked for training, and count of observed
     */
    static class SampleInfo {

        private final long classCount;
        private long training;
        private long observed;

        SampleInfo(long classCount) {
            this.classCount = classCount;
        }
    }
}
