/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.vectors;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class VectorsFeatureSetUsageTests extends AbstractWireSerializingTestCase<VectorsFeatureSetUsage> {

    @Override
    protected VectorsFeatureSetUsage createTestInstance() {
        boolean available = randomBoolean();
        boolean enabled = randomBoolean();
        if (available && enabled) {
            return new VectorsFeatureSetUsage(available, randomIntBetween(0, 100000), randomIntBetween(0, 1024));
        } else {
            return new VectorsFeatureSetUsage(available, 0, 0);
        }
    }

    @Override
    protected VectorsFeatureSetUsage mutateInstance(VectorsFeatureSetUsage instance) throws IOException {
        boolean available = instance.available();
        boolean enabled = instance.enabled();
        int numDenseVectorFields = instance.numDenseVectorFields();
        int avgDenseVectorDims = instance.avgDenseVectorDims();

        if (available == false || enabled == false) {
            available = true;
            enabled = true;
        }
        numDenseVectorFields = randomValueOtherThan(numDenseVectorFields, () -> randomIntBetween(0, 100000));
        avgDenseVectorDims = randomValueOtherThan(avgDenseVectorDims, () -> randomIntBetween(0, 1024));
        return new VectorsFeatureSetUsage(available, numDenseVectorFields, avgDenseVectorDims);
    }

    @Override
    protected Writeable.Reader<VectorsFeatureSetUsage> instanceReader() {
        return VectorsFeatureSetUsage::new;
    }

}
