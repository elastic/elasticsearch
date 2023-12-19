/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.client.Node;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;

import java.util.Objects;

interface ClientYamlNodeSelector {

    interface NodeSelectorWithContext {
        void select(Iterable<Node> nodes, ClientYamlTestExecutionContext context);
    }

    static ClientYamlNodeSelector withoutContext(String selectorDescription, NodeSelector nodeSelector) {
        return new NoContextNodeSelector(nodeSelector, selectorDescription);
    }

    static ClientYamlNodeSelector withoutContext(NodeSelector nodeSelector) {
        return new NoContextNodeSelector(nodeSelector, nodeSelector.toString());
    }

    static ClientYamlNodeSelector withContext(String selectorDescription, NodeSelectorWithContext nodeSelector) {
        return new ClientYamlNodeSelector() {
            @Override
            public void select(Iterable<Node> nodes, ClientYamlTestExecutionContext context) {
                nodeSelector.select(nodes, context);
            }

            @Override
            public NodeSelector bind(ClientYamlTestExecutionContext executionContext) {
                return nodes -> nodeSelector.select(nodes, executionContext);
            }

            @Override
            public String toString() {
                return selectorDescription;
            }
        };
    }

    /**
     * Selector that composes two selectors, running the "right" most selector
     * first and then running the "left" selector on the results of the "right"
     * selector.
     */
    static ClientYamlNodeSelector compose(ClientYamlNodeSelector lhs, ClientYamlNodeSelector rhs) {
        Objects.requireNonNull(lhs, "lhs is required");
        Objects.requireNonNull(rhs, "rhs is required");

        // . as in haskell's "compose" operator
        var description = lhs + "." + rhs;

        var needsContext = lhs instanceof NoContextNodeSelector == false || rhs instanceof NoContextNodeSelector == false;

        if (needsContext) {
            return withContext(description, (nodes, context) -> {
                rhs.select(nodes, context);
                lhs.select(nodes, context);
            });
        } else {
            return withoutContext(description, (nodes) -> {
                rhs.select(nodes, null);
                lhs.select(nodes, null);
            });
        }
    }

    class NoContextNodeSelector implements ClientYamlNodeSelector {
        private final NodeSelector nodeSelector;
        private final String nodeSelectorString;

        NoContextNodeSelector(NodeSelector nodeSelector, String nodeSelectorString) {
            this.nodeSelector = nodeSelector;
            this.nodeSelectorString = nodeSelectorString;
        }

        @Override
        public void select(Iterable<Node> nodes, ClientYamlTestExecutionContext context) {
            nodeSelector.select(nodes);
        }

        @Override
        public NodeSelector bind(ClientYamlTestExecutionContext executionContext) {
            return nodeSelector;
        }

        @Override
        public String toString() {
            return nodeSelectorString;
        }
    }

    ClientYamlNodeSelector ANY = withoutContext(NodeSelector.ANY);

    void select(Iterable<Node> nodes, ClientYamlTestExecutionContext context);

    NodeSelector bind(ClientYamlTestExecutionContext executionContext);
}
