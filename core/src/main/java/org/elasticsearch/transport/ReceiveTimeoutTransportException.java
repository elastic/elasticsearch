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

import java.io.IOException;

/**
 *
 */
public class ReceiveTimeoutTransportException extends ActionTransportException {

    public ReceiveTimeoutTransportException(DiscoveryNode node, String action, String msg) {
        super(node.getName(), node.getAddress(), action, msg, null);
    }

    public ReceiveTimeoutTransportException(StreamInput in) throws IOException {
        super(in);
    }

//    @Override public Throwable fillInStackTrace() {
//        return fillStack();
//    }
}
