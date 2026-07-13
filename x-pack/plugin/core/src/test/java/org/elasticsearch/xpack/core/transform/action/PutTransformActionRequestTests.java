/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.transform.action.PutTransformAction.Request;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.junit.Before;

public class PutTransformActionRequestTests extends AbstractWireSerializingTransformTestCase<Request> {
    private String transformId;

    @Before
    public void setupTransformId() {
        transformId = randomAlphaOfLengthBetween(1, 10);
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        TransformConfig config = TransformConfigTests.randomTransformConfigWithoutHeaders(transformId);
        return new Request(config, randomBoolean(), randomTimeValue());
    }

    @Override
    protected Request mutateInstance(Request instance) {
        TransformConfig config = instance.getConfig();
        boolean deferValidation = instance.isDeferValidation();
        TimeValue timeout = instance.ackTimeout();

        switch (between(0, 2)) {
            case 0 -> config = new TransformConfig.Builder(config).setId(config.getId() + randomAlphaOfLengthBetween(1, 5)).build();
            case 1 -> deferValidation ^= true;
            case 2 -> timeout = new TimeValue(timeout.duration() + randomLongBetween(1, 5), timeout.timeUnit());
            default -> throw new AssertionError("Illegal randomization branch");
        }

        return new Request(config, deferValidation, timeout);
    }

    @Override
    protected Request mutateInstanceForVersion(Request instance, TransportVersion version) {
        // SourceConfig has version-gated fields (projectRouting, indicesOptions); delegate to the shared
        // TransformConfig helper that drops them for older versions so the BWC baseline matches the wire round-trip.
        return new Request(
            TransformConfigTests.mutateForVersion(instance.getConfig(), version),
            instance.isDeferValidation(),
            instance.ackTimeout()
        );
    }
}
