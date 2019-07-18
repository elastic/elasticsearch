/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.spatial;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class SpatialFeatureSetUsageTests extends AbstractWireSerializingTestCase<SpatialFeatureSetUsage> {

    @Override
    protected SpatialFeatureSetUsage createTestInstance() {
        boolean available = randomBoolean();
        boolean enabled = randomBoolean();
        return new SpatialFeatureSetUsage(available, enabled);
    }

    @Override
    protected SpatialFeatureSetUsage mutateInstance(SpatialFeatureSetUsage instance) throws IOException {
        boolean available = instance.available();
        boolean enabled = instance.enabled();
        switch (between(0, 1)) {
            case 0:
                available = available == false;
                break;
            case 1:
                enabled = enabled == false;
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new SpatialFeatureSetUsage(available, enabled);
    }

    @Override
    protected Writeable.Reader<SpatialFeatureSetUsage> instanceReader() {
        return SpatialFeatureSetUsage::new;
    }

}
