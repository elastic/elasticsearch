/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.flattened;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class FlattenedFeatureSetUsageTests extends AbstractWireSerializingTestCase<FlattenedFeatureSetUsage> {

    @Override
    protected FlattenedFeatureSetUsage createTestInstance() {
        return new FlattenedFeatureSetUsage(randomBoolean(), randomBoolean(), randomIntBetween(0, 1000));
    }

    @Override
    protected FlattenedFeatureSetUsage mutateInstance(FlattenedFeatureSetUsage instance) throws IOException {

        boolean available = instance.available();
        boolean enabled = instance.enabled();
        int fieldCount = instance.fieldCount();

        switch (between(0, 2)) {
            case 0:
                available = !available;
                break;
            case 1:
                enabled = !enabled;
                break;
            case 2:
                fieldCount = randomValueOtherThan(instance.fieldCount(), () -> randomIntBetween(0, 1000));
                break;
        }

        return new FlattenedFeatureSetUsage(available, enabled, fieldCount);
    }

    @Override
    protected Writeable.Reader<FlattenedFeatureSetUsage> instanceReader() {
        return FlattenedFeatureSetUsage::new;
    }

}
