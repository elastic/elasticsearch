/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class MlProcessorsTests extends ESTestCase {

    public void testGet() {
        var node = DiscoveryNodeUtils.builder("foo").attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "8.0")).build();
        var processor = MlProcessors.get(node, 1);
        assertThat(processor.count(), equalTo(8.0));
    }

    public void testGetWithScale() {
        var node = DiscoveryNodeUtils.builder("foo").attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "8.0")).build();
        var processor = MlProcessors.get(node, 2);
        assertThat(processor.count(), equalTo(4.0));
    }

    public void testGetWithNull() {
        var node = DiscoveryNodeUtils.builder("foo").attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "8.0")).build();
        var processor = MlProcessors.get(node, null);
        assertThat(processor.count(), equalTo(8.0));
    }
}
