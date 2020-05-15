/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.dataframe.process.crossvalidation;

import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Given a dependent variable, randomly splits the dataset trying
 * to preserve the proportion of each class in the training sample.
 */
public class StratifiedCrossValidationSplitter implements CrossValidationSplitter {

    private final int dependentVariableIndex;
    private final double samplingRatio;
    private final Random random;
    private final Map<String, ClassSample> classSamples;

    public StratifiedCrossValidationSplitter(List<String> fieldNames, String dependentVariable, Map<String, Long> classCardinalities,
                                             double trainingPercent, long randomizeSeed) {
        assert trainingPercent >= 1.0 && trainingPercent <= 100.0;
        this.dependentVariableIndex = findDependentVariableIndex(fieldNames, dependentVariable);
        this.samplingRatio = trainingPercent / 100.0;
        this.random = new Random(randomizeSeed);
        this.classSamples = new HashMap<>();
        classCardinalities.entrySet().forEach(entry -> classSamples.put(entry.getKey(), new ClassSample(entry.getValue())));
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

        String classValue = row[dependentVariableIndex];
        ClassSample sample = classSamples.get(classValue);
        if (sample == null) {
            throw new IllegalStateException("Unknown class [" + classValue + "]; expected one of " + classSamples.keySet());
        }

        // We ensure the target sample count is at least 1 as if the cardinality
        // is too low we might get a target of zero and, thus, no samples of the whole class
        double targetSampleCount = Math.max(1.0, samplingRatio * sample.cardinality);

        // The idea here is that the probability increases as the chances we have to get the target proportion
        // for a class decreases.
        double p = (targetSampleCount - sample.training) / (sample.cardinality - sample.observed);

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

    private static class ClassSample {

        private final long cardinality;
        private long training;
        private long observed;

        private ClassSample(long cardinality) {
            this.cardinality = cardinality;
        }
    }
}
