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

    @Override
    protected TrainedModelSizeStats mutateInstance(TrainedModelSizeStats instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    // @Override
    // protected TrainedModelSizeStats mutateInstanceForVersion(TrainedModelSizeStats instance, TransportVersion version) {
    // TrainedModelSizeStats.Builder builder = new TrainedModelSizeStats.Builder(instance);
    // if(version.before(TrainedModelConfig.VERSION_ALLOCATION_MEMORY_ADDED)) {
    // builder.setPerDeploymentMemoryBytes(0);
    // builder.setPerAllocationMemoryBytes(0);
    // } else {
    // builder.setPerDeploymentMemoryBytes(randomNonNegativeLong());
    // builder.setPerAllocationMemoryBytes(randomNonNegativeLong());
    // }
    // return builder.build();
    // }

    public static TrainedModelSizeStats createRandom() {
        return new TrainedModelSizeStats(randomNonNegativeLong(), randomNonNegativeLong());
    }
}
