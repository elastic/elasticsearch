/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;

public class TransportMlInfoActionTests extends ESTestCase {

    public void testAreMlNodesBiggestSize() {
        boolean expectedResult = randomBoolean();
        long mlNodeSize = randomLongBetween(10000000L, 10000000000L);
        long biggestSize = expectedResult ? mlNodeSize : mlNodeSize * randomLongBetween(2, 5);
        int numMlNodes = randomIntBetween(2, 4);
        var nodes = Stream.generate(
            () -> DiscoveryNodeUtils.builder("node")
                .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                .attributes(Map.of(MachineLearning.MACHINE_MEMORY_NODE_ATTR, Long.toString(mlNodeSize)))
                .build()
        ).limit(numMlNodes).toList();
        assertThat(TransportMlInfoAction.areMlNodesBiggestSize(ByteSizeValue.ofBytes(biggestSize), nodes), is(expectedResult));
    }
}
