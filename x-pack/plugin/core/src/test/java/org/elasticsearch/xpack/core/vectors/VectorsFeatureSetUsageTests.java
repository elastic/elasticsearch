/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.vectors;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class VectorsFeatureSetUsageTests extends AbstractWireSerializingTestCase<VectorsFeatureSetUsage> {

    @Override
    protected VectorsFeatureSetUsage createTestInstance() {
        return new VectorsFeatureSetUsage(randomIntBetween(0, 100000), randomIntBetween(0, 100000), randomIntBetween(0, 1024));
    }

    @Override
    protected VectorsFeatureSetUsage mutateInstance(VectorsFeatureSetUsage instance) throws IOException {
        int numDenseVectorFields = instance.numDenseVectorFields();
        int numSparseVectorFields = instance.numSparseVectorFields();
        int avgDenseVectorDims = instance.avgDenseVectorDims();
        numDenseVectorFields = randomValueOtherThan(numDenseVectorFields, () -> randomIntBetween(0, 100000));
        numSparseVectorFields = randomValueOtherThan(numSparseVectorFields, () -> randomIntBetween(0, 100000));
        avgDenseVectorDims = randomValueOtherThan(avgDenseVectorDims, () -> randomIntBetween(0, 1024));
        return new VectorsFeatureSetUsage(numDenseVectorFields, numSparseVectorFields, avgDenseVectorDims);
    }

    @Override
    protected Writeable.Reader<VectorsFeatureSetUsage> instanceReader() {
        return VectorsFeatureSetUsage::new;
    }

}
