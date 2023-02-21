/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class GetAutoscalingCapacityActionRequestWireSerializingTests extends AbstractWireSerializingTestCase<
    GetAutoscalingCapacityAction.Request> {

    @Override
    protected Writeable.Reader<GetAutoscalingCapacityAction.Request> instanceReader() {
        return GetAutoscalingCapacityAction.Request::new;
    }

    @Override
    protected GetAutoscalingCapacityAction.Request createTestInstance() {
        return new GetAutoscalingCapacityAction.Request();
    }

    @Override
    protected GetAutoscalingCapacityAction.Request mutateInstance(GetAutoscalingCapacityAction.Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

}
