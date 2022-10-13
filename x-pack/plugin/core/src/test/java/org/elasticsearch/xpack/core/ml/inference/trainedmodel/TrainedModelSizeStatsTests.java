/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class TrainedModelSizeStatsTests extends AbstractWireSerializingTestCase<TrainedModelSizeStats> {

    @Override
    protected Writeable.Reader<TrainedModelSizeStats> instanceReader() {
        return TrainedModelSizeStats::new;
    }

    @Override
    protected TrainedModelSizeStats createTestInstance() {
        return createRandom();
    }

    public static TrainedModelSizeStats createRandom() {
        return new TrainedModelSizeStats(randomNonNegativeLong(), randomNonNegativeLong());
    }
}
