/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;

import java.util.SortedMap;
import java.util.SortedSet;

public class PutAutoscalingPolicyActionRequestWireSerializingTests extends AbstractWireSerializingTestCase<
    PutAutoscalingPolicyAction.Request> {

    @Override
    protected Writeable.Reader<PutAutoscalingPolicyAction.Request> instanceReader() {
        return PutAutoscalingPolicyAction.Request::new;
    }

    @Override
    protected PutAutoscalingPolicyAction.Request createTestInstance() {
        return TransportPutAutoscalingPolicyActionTests.randomPutAutoscalingPolicyRequest();
    }

    @Override
    protected PutAutoscalingPolicyAction.Request mutateInstance(PutAutoscalingPolicyAction.Request instance) {
        String name = instance.name();
        SortedSet<String> roles = instance.roles();
        SortedMap<String, Settings> deciders = instance.deciders();
        switch (between(0, 2)) {
            case 0 -> name = randomValueOtherThan(name, () -> randomAlphaOfLength(8));
            case 1 -> roles = randomValueOtherThan(roles, AutoscalingTestCase::randomRoles);
            case 2 -> deciders = randomValueOtherThan(
                deciders,
                () -> randomBoolean() ? AutoscalingTestCase.randomAutoscalingDeciders() : null
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new PutAutoscalingPolicyAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, name, roles, deciders);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return AutoscalingTestCase.getAutoscalingNamedWriteableRegistry();
    }

}
