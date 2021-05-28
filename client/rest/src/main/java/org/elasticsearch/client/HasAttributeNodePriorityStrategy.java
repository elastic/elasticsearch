/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A {@link NodePriorityStrategy} that prioritize nodes that have a particular value
 * for an attribute.
 */
public class HasAttributeNodePriorityStrategy implements NodePriorityStrategy {
    private final String key;
    private final String value;

    public HasAttributeNodePriorityStrategy(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public List<List<Node>> groupByPriority(List<Node> nodes) {
        List<Node> highPriorityNodes = new ArrayList<>();
        List<Node> lowPriorityNodes = new ArrayList<>();

        nodes.forEach(node -> {
            Map<String, List<String>> allAttributes = node.getAttributes();
            if (allAttributes == null) return;
            List<String> values = allAttributes.get(key);
            if (values == null || false == values.contains(value)) {
                lowPriorityNodes.add(node);
            } else {
                highPriorityNodes.add(node);
            }
        });

        return Arrays.asList(highPriorityNodes, lowPriorityNodes);
    }
}
