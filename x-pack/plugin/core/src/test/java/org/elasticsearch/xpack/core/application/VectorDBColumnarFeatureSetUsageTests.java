/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.application;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class VectorDBColumnarFeatureSetUsageTests extends AbstractWireSerializingTestCase<VectorDBColumnarFeatureSetUsage> {

    @Override
    protected Writeable.Reader<VectorDBColumnarFeatureSetUsage> instanceReader() {
        return VectorDBColumnarFeatureSetUsage::new;
    }

    @Override
    protected VectorDBColumnarFeatureSetUsage createTestInstance() {
        return new VectorDBColumnarFeatureSetUsage(randomBoolean(), randomBoolean(), randomIntBetween(0, 1000), randomNonNegativeLong());
    }

    @Override
    protected VectorDBColumnarFeatureSetUsage mutateInstance(VectorDBColumnarFeatureSetUsage instance) throws IOException {
        boolean available = instance.available();
        boolean enabled = instance.enabled();
        int indicesCount = instance.indicesCount();
        long numDocs = instance.numDocs();
        switch (between(0, 3)) {
            case 0 -> available = available == false;
            case 1 -> enabled = enabled == false;
            case 2 -> indicesCount = randomValueOtherThan(indicesCount, () -> randomIntBetween(0, 1000));
            case 3 -> numDocs = randomValueOtherThan(numDocs, () -> randomNonNegativeLong());
            default -> throw new AssertionError("unreachable");
        }
        return new VectorDBColumnarFeatureSetUsage(available, enabled, indicesCount, numDocs);
    }
}
