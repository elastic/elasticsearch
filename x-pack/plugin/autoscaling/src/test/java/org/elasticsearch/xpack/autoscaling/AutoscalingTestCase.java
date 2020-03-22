/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.test.ESTestCase;

public abstract class AutoscalingTestCase extends ESTestCase {

    protected AutoscalingDecision.SingleAutoscalingDecision randomAutoscalingDecision() {
        return randomAutoscalingDecisionOfType(randomFrom(AutoscalingDecision.Type.values()));
    }

    protected AutoscalingDecision.SingleAutoscalingDecision randomAutoscalingDecisionOfType(final AutoscalingDecision.Type type) {
        return new AutoscalingDecision.SingleAutoscalingDecision(randomAlphaOfLength(8), type, randomAlphaOfLength(8));
    }

}
