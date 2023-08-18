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
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return AutoscalingTestCase.getAutoscalingNamedWriteableRegistry();
    }

}
