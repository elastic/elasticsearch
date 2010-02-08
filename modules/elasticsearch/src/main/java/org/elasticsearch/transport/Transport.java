/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.cluster.node.Node;
import org.elasticsearch.util.component.LifecycleComponent;
import org.elasticsearch.util.io.Streamable;
import org.elasticsearch.util.transport.BoundTransportAddress;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public interface Transport extends LifecycleComponent<Transport> {

    class Helper {
        public static final byte TRANSPORT_TYPE = 1;
        public static final byte RESPONSE_TYPE = 1 << 1;

        public static boolean isRequest(byte value) {
            return (value & TRANSPORT_TYPE) == 0;
        }

        public static byte setRequest(byte value) {
            value &= ~TRANSPORT_TYPE;
            return value;
        }

        public static byte setResponse(byte value) {
            value |= TRANSPORT_TYPE;
            return value;
        }

        public static boolean isError(byte value) {
            return (value & RESPONSE_TYPE) != 0;
        }

        public static byte setError(byte value) {
            value |= RESPONSE_TYPE;
            return value;
        }

    }

    void transportServiceAdapter(TransportServiceAdapter service);

    BoundTransportAddress boundAddress();

    void nodesAdded(Iterable<Node> nodes);

    void nodesRemoved(Iterable<Node> nodes);

    <T extends Streamable> void sendRequest(Node node, long requestId, String action,
                                            Streamable message, TransportResponseHandler<T> handler) throws IOException, TransportException;
}
