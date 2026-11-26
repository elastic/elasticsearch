/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction.Request;

import static org.elasticsearch.xpack.core.transform.action.GetTransformAction.DANGLING_TASKS;

public class GetTransformActionRequestTests extends AbstractBWCWireSerializationTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        if (randomBoolean()) {
            return new Request(Metadata.ALL);
        }
        return new Request(randomAlphaOfLengthBetween(1, 20), randomBoolean(), randomPositiveTimeValue());
    }

    @Override
    protected Request mutateInstance(Request instance) {
        if (Metadata.ALL.equals(instance.getId())) {
            return new Request(
                randomAlphaOfLengthBetween(1, 20),
                instance.checkForDanglingTasks(),
                instance.timeout()
            );
        }
        return switch (between(0, 2)) {
            case 0 -> new Request(
                randomValueOtherThan(instance.getId(), () -> randomAlphaOfLengthBetween(1, 20)),
                instance.checkForDanglingTasks(),
                instance.timeout()
            );
            case 1 -> new Request(
                instance.getId(),
                randomValueOtherThan(instance.checkForDanglingTasks(), ESTestCase::randomBoolean),
                instance.timeout()
            );
            default -> new Request(
                instance.getId(),
                instance.checkForDanglingTasks(),
                randomValueOtherThan(instance.timeout(), ESTestCase::randomPositiveTimeValue)
            );
        };
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request mutateInstanceForVersion(Request instance, TransportVersion version) {
        return version.supports(DANGLING_TASKS) ? instance : new Request(instance.getId(), true, TimeValue.MAX_VALUE);
    }
}
