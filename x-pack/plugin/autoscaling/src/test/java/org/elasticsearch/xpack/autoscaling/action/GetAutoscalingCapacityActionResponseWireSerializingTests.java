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
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResults;

import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.xpack.autoscaling.AutoscalingTestCase.randomAutoscalingDeciderResults;

public class GetAutoscalingCapacityActionResponseWireSerializingTests extends AbstractWireSerializingTestCase<
    GetAutoscalingCapacityAction.Response> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return AutoscalingTestCase.getAutoscalingNamedWriteableRegistry();
    }

    @Override
    protected Writeable.Reader<GetAutoscalingCapacityAction.Response> instanceReader() {
        return GetAutoscalingCapacityAction.Response::new;
    }

    @Override
    protected GetAutoscalingCapacityAction.Response createTestInstance() {
        final int numberOfPolicies = randomIntBetween(1, 8);
        final SortedMap<String, AutoscalingDeciderResults> results = new TreeMap<>();
        for (int i = 0; i < numberOfPolicies; i++) {
            results.put(randomAlphaOfLength(8), randomAutoscalingDeciderResults());
        }
        return new GetAutoscalingCapacityAction.Response(Collections.unmodifiableSortedMap(results));
    }

    @Override
    protected GetAutoscalingCapacityAction.Response mutateInstance(GetAutoscalingCapacityAction.Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

}
