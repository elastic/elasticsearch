/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.capacity;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;

import java.util.SortedMap;
import java.util.TreeMap;

public class AutoscalingDeciderResultsWireSerializingTests extends AbstractWireSerializingTestCase<AutoscalingDeciderResults> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return AutoscalingTestCase.getAutoscalingNamedWriteableRegistry();
    }

    @Override
    protected Writeable.Reader<AutoscalingDeciderResults> instanceReader() {
        return AutoscalingDeciderResults::new;
    }

    @Override
    protected AutoscalingDeciderResults createTestInstance() {
        return AutoscalingTestCase.randomAutoscalingDeciderResults();
    }

    @Override
    protected AutoscalingDeciderResults mutateInstance(AutoscalingDeciderResults instance) {
        AutoscalingCapacity currentCapacity = instance.currentCapacity();
        final SortedMap<String, AutoscalingDeciderResult> results = new TreeMap<>(instance.results());
        switch (between(0, 1)) {
            case 0 -> currentCapacity = randomValueOtherThan(currentCapacity, AutoscalingTestCase::randomAutoscalingCapacity);
            case 1 -> {
                if (randomBoolean()) {
                    results.put(
                        randomValueOtherThanMany(results::containsKey, () -> randomAlphaOfLength(8)),
                        AutoscalingTestCase.randomAutoscalingDeciderResult()
                    );
                } else {
                    final String key = randomFrom(results.keySet());
                    results.put(key, randomValueOtherThan(results.get(key), AutoscalingTestCase::randomAutoscalingDeciderResult));
                }
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new AutoscalingDeciderResults(currentCapacity, instance.currentNodes(), results);
    }

}
