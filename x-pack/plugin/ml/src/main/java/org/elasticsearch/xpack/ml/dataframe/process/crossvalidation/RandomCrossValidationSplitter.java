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
 * A cross validation splitter that randomly clears the dependent variable value
 * in order to split the dataset in training and test data.
 * This relies on the fact that when the dependent variable field
 * is empty, then the row is not used for training but only to make predictions.
 */
class RandomCrossValidationSplitter implements CrossValidationSplitter {

    private final int dependentVariableIndex;
    private final double trainingPercent;
    private final Random random;
    private boolean isFirstRow = true;

    RandomCrossValidationSplitter(List<String> fieldNames, String dependentVariable, double trainingPercent, long randomizeSeed) {
        assert trainingPercent >= 1.0 && trainingPercent <= 100.0;
        this.dependentVariableIndex = findDependentVariableIndex(fieldNames, dependentVariable);
        this.trainingPercent = trainingPercent;
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
        if (canBeUsedForTraining(row) && isPickedForTraining()) {
            incrementTrainingDocs.run();
        } else {
            row[dependentVariableIndex] = DataFrameDataExtractor.NULL_VALUE;
            incrementTestDocs.run();
        }
    }

    private boolean canBeUsedForTraining(String[] row) {
        return row[dependentVariableIndex] != DataFrameDataExtractor.NULL_VALUE;
    }

    private boolean isPickedForTraining() {
        if (isFirstRow) {
            // Let's make sure we have at least one training row
            isFirstRow = false;
            return true;
        }
        return random.nextDouble() * 100 <= trainingPercent;
    }
}
