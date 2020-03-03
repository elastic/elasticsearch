/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process.customprocessing;

import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.List;
import java.util.Random;

/**
 * A processor that randomly clears the dependent variable value
 * in order to split the dataset in training and validation data.
 * This relies on the fact that when the dependent variable field
 * is empty, then the row is not used for training but only to make predictions.
 */
class DatasetSplittingCustomProcessor implements CustomProcessor {

    private static final String EMPTY = "";

    private final int dependentVariableIndex;
    private final double trainingPercent;
    private final Random random;
    private boolean isFirstRow = true;

    DatasetSplittingCustomProcessor(List<String> fieldNames, String dependentVariable, double trainingPercent, long randomizeSeed) {
        this.dependentVariableIndex = findDependentVariableIndex(fieldNames, dependentVariable);
        this.trainingPercent = trainingPercent;
        this.random = new Random(randomizeSeed);
    }

    private static int findDependentVariableIndex(List<String> fieldNames, String dependentVariable) {
        for (int i = 0; i < fieldNames.size(); i++) {
            if (fieldNames.get(i).equals(dependentVariable)) {
                return i;
            }
        }
        throw ExceptionsHelper.serverError("Could not find dependent variable [" + dependentVariable + "] in fields " + fieldNames);
    }

    @Override
    public void process(String[] row) {
        if (canBeUsedForTraining(row)) {
            if (isFirstRow) {
                // Let's make sure we have at least one training row
                isFirstRow = false;
            } else if (isRandomlyExcludedFromTraining()) {
                row[dependentVariableIndex] = EMPTY;
            }
        }
    }

    private boolean canBeUsedForTraining(String[] row) {
        return row[dependentVariableIndex].length() > 0;
    }

    private boolean isRandomlyExcludedFromTraining() {
        return random.nextDouble() * 100 > trainingPercent;
    }
}
