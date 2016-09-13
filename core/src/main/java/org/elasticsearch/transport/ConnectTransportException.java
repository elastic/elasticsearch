/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 *
 */
public class ConnectTransportException extends ActionTransportException {

    private final DiscoveryNode node;

    public ConnectTransportException(DiscoveryNode node, String msg) {
        this(node, msg, null, null);
    }

    public ConnectTransportException(DiscoveryNode node, String msg, String action) {
        this(node, msg, action, null);
    }

    public ConnectTransportException(DiscoveryNode node, String msg, Throwable cause) {
        this(node, msg, null, cause);
    }

    public ConnectTransportException(DiscoveryNode node, String msg, String action, Throwable cause) {
        super(node == null ? null : node.getName(), node == null ? null : node.getAddress(), action, msg, cause);
        this.node = node;
    }

    public ConnectTransportException(StreamInput in) throws IOException {
        super(in);
        node = in.readOptionalWriteable(DiscoveryNode::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(node);
    }

    public DiscoveryNode node() {
        return node;
    }
}
