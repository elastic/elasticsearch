/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.traintestsplit;

import java.util.List;

public class SingleClassReservoirTrainTestSplitter extends AbstractReservoirTrainTestSplitter {

    private final SampleInfo sampleInfo;

    SingleClassReservoirTrainTestSplitter(List<String> fieldNames, String dependentVariable, double trainingPercent,
                                          long randomizeSeed, long classCount) {
        super(fieldNames, dependentVariable, trainingPercent, randomizeSeed);
        sampleInfo = new SampleInfo(classCount);
    }

    @Override
    protected SampleInfo getSampleInfo(String[] row) {
        return sampleInfo;
    }
}
