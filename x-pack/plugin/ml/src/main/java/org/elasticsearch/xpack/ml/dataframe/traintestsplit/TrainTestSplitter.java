/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.traintestsplit;

/**
 * Processes rows in order to split the dataset in training and test subsets
 */
public interface TrainTestSplitter {

    boolean isTraining(String[] row);
}
