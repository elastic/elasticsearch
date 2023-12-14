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

interface ContextNodeSelector {
    void select(Iterable<Node> nodes, ClientYamlTestExecutionContext context);

    ContextNodeSelector ANY = (nodes, context) -> {};

    default NodeSelector bind(ClientYamlTestExecutionContext executionContext) {
        var me = this;
        return nodes -> me.select(nodes, executionContext);
    }

    static ContextNodeSelector wrap(NodeSelector nodeSelector) {
        return (nodes, context) -> nodeSelector.select(nodes);
    }
}
