/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.dataframe.process.crossvalidation;

import java.util.List;

public class SingleClassReservoirCrossValidationSplitter extends AbstractReservoirCrossValidationSplitter {

    private final SampleInfo sampleInfo;

    SingleClassReservoirCrossValidationSplitter(List<String> fieldNames, String dependentVariable, double trainingPercent,
                                                long randomizeSeed, long classCount) {
        super(fieldNames, dependentVariable, trainingPercent, randomizeSeed);
        sampleInfo = new SampleInfo(classCount);
    }

    @Override
    protected SampleInfo getSampleInfo(String[] row) {
        return sampleInfo;
    }
}
