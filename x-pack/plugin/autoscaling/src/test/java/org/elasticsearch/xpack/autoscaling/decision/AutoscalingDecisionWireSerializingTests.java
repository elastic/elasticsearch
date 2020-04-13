/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;

public class AutoscalingDecisionWireSerializingTests extends AbstractWireSerializingTestCase<AutoscalingDecision> {

    @Override
    protected Writeable.Reader<AutoscalingDecision> instanceReader() {
        return AutoscalingDecision::new;
    }

    @Override
    protected AutoscalingDecision createTestInstance() {
        return AutoscalingTestCase.randomAutoscalingDecision();
    }

}
