/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.geo;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class GeoFeatureSetUsageTests extends AbstractWireSerializingTestCase<GeoFeatureSetUsage> {

    @Override
    protected GeoFeatureSetUsage createTestInstance() {
        boolean available = randomBoolean();
        boolean enabled = randomBoolean();
        return new GeoFeatureSetUsage(available, enabled);
    }

    @Override
    protected GeoFeatureSetUsage mutateInstance(GeoFeatureSetUsage instance) throws IOException {
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
        return new GeoFeatureSetUsage(available, enabled);
    }

    @Override
    protected Writeable.Reader<GeoFeatureSetUsage> instanceReader() {
        return GeoFeatureSetUsage::new;
    }

}
