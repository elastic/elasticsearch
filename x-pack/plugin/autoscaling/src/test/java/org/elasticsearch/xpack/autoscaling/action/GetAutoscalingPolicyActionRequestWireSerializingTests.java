/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class GetAutoscalingPolicyActionRequestWireSerializingTests extends AbstractWireSerializingTestCase<
    GetAutoscalingPolicyAction.Request> {

    @Override
    protected Writeable.Reader<GetAutoscalingPolicyAction.Request> instanceReader() {
        return GetAutoscalingPolicyAction.Request::new;
    }

    @Override
    protected GetAutoscalingPolicyAction.Request createTestInstance() {
        return new GetAutoscalingPolicyAction.Request(randomAlphaOfLength(8));
    }

}
