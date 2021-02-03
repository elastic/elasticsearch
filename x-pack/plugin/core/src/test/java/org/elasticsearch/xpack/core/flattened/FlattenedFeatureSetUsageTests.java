/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.flattened;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class FlattenedFeatureSetUsageTests extends AbstractWireSerializingTestCase<FlattenedFeatureSetUsage> {

    @Override
    protected FlattenedFeatureSetUsage createTestInstance() {
        return new FlattenedFeatureSetUsage(randomIntBetween(0, 1000));
    }

    @Override
    protected FlattenedFeatureSetUsage mutateInstance(FlattenedFeatureSetUsage instance) throws IOException {
        int fieldCount = randomValueOtherThan(instance.fieldCount(), () -> randomIntBetween(0, 1000));
        return new FlattenedFeatureSetUsage(fieldCount);
    }

    @Override
    protected Writeable.Reader<FlattenedFeatureSetUsage> instanceReader() {
        return FlattenedFeatureSetUsage::new;
    }

}
