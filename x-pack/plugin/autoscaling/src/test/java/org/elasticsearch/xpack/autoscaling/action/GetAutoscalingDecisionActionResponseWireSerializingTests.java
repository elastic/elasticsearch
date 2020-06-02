/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecisions;

import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.xpack.autoscaling.AutoscalingTestCase.randomAutoscalingDecisions;

public class GetAutoscalingDecisionActionResponseWireSerializingTests extends AbstractWireSerializingTestCase<
    GetAutoscalingDecisionAction.Response> {

    @Override
    protected Writeable.Reader<GetAutoscalingDecisionAction.Response> instanceReader() {
        return GetAutoscalingDecisionAction.Response::new;
    }

    @Override
    protected GetAutoscalingDecisionAction.Response createTestInstance() {
        final int numberOfPolicies = randomIntBetween(1, 8);
        final SortedMap<String, AutoscalingDecisions> decisions = new TreeMap<>();
        for (int i = 0; i < numberOfPolicies; i++) {
            decisions.put(randomAlphaOfLength(8), randomAutoscalingDecisions());
        }
        return new GetAutoscalingDecisionAction.Response(Collections.unmodifiableSortedMap(decisions));
    }

}
