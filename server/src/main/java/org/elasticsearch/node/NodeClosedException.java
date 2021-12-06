/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.node;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * An exception indicating that node is closed.
 *
 *
 */
public class NodeClosedException extends ElasticsearchException {

    public NodeClosedException(DiscoveryNode node) {
        super("node closed " + node);
    }

    public NodeClosedException(StreamInput in) throws IOException {
        super(in);
    }
}
