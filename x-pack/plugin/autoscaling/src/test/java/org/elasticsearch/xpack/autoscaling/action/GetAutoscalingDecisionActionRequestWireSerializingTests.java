/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class GetAutoscalingDecisionActionRequestWireSerializingTests extends AbstractWireSerializingTestCase<
    GetAutoscalingDecisionAction.Request> {

    @Override
    protected Writeable.Reader<GetAutoscalingDecisionAction.Request> instanceReader() {
        return GetAutoscalingDecisionAction.Request::new;
    }

    @Override
    protected GetAutoscalingDecisionAction.Request createTestInstance() {
        return new GetAutoscalingDecisionAction.Request();
    }

}
