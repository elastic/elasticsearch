/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Map;

public class HealthApiFeatureSetUsageTests extends AbstractWireSerializingTestCase<HealthApiFeatureSetUsage> {

    @Override
    protected HealthApiFeatureSetUsage createTestInstance() {
        return new HealthApiFeatureSetUsage(
            true,
            true,
            Map.of(
                "invocations",
                Map.of("total", randomNonNegativeLong(), "verbose_true", randomNonNegativeLong()),
                "indicators",
                Map.of("red", Map.of("ilm", 10))
            )
        );
    }

    @Override
    protected HealthApiFeatureSetUsage mutateInstance(HealthApiFeatureSetUsage instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected Writeable.Reader<HealthApiFeatureSetUsage> instanceReader() {
        return HealthApiFeatureSetUsage::new;
    }

}
