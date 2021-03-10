/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.ElasticsearchWrapperException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class SendRequestTransportException extends ActionTransportException implements ElasticsearchWrapperException {

    public SendRequestTransportException(DiscoveryNode node, String action, Throwable cause) {
        super(node == null ? null : node.getName(), node == null ? null : node.getAddress(), action, cause);
    }

    public SendRequestTransportException(StreamInput in) throws IOException {
        super(in);
    }
}
