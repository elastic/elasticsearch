/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;

import static org.elasticsearch.xpack.autoscaling.AutoscalingTestCase.randomAutoscalingPolicy;

public class GetAutoscalingPolicyActionResponseWireSerializingTests extends AbstractWireSerializingTestCase<
    GetAutoscalingPolicyAction.Response> {

    @Override
    protected Writeable.Reader<GetAutoscalingPolicyAction.Response> instanceReader() {
        return GetAutoscalingPolicyAction.Response::new;
    }

    @Override
    protected GetAutoscalingPolicyAction.Response createTestInstance() {
        return new GetAutoscalingPolicyAction.Response(randomAutoscalingPolicy());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return AutoscalingTestCase.getAutoscalingNamedWriteableRegistry();
    }

}
