/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process.customprocessing;

import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.DataFrameAnalysis;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;

import java.util.List;
import java.util.Objects;

public class CustomProcessorFactory {

    private final List<String> fieldNames;

    public CustomProcessorFactory(List<String> fieldNames) {
        this.fieldNames = Objects.requireNonNull(fieldNames);
    }

    public CustomProcessor create(DataFrameAnalysis analysis) {
        if (analysis instanceof Regression) {
            Regression regression = (Regression) analysis;
            return new DatasetSplittingCustomProcessor(
                fieldNames, regression.getDependentVariable(), regression.getTrainingPercent(), regression.getRandomizeSeed());
        }
        if (analysis instanceof Classification) {
            Classification classification = (Classification) analysis;
            return new DatasetSplittingCustomProcessor(
                fieldNames, classification.getDependentVariable(), classification.getTrainingPercent(), classification.getRandomizeSeed());
        }
        return row -> {};
    }
}
