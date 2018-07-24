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
package org.elasticsearch.test.transport;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectionManager;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class MockConnectionManager extends ConnectionManager {

    private final ConcurrentHashMap<TransportAddress, MockTransportService.SendRequestBehavior> sendBehaviors = new ConcurrentHashMap<>();
    private volatile MockTransportService.SendRequestBehavior defaultSendRequest = null;

    MockConnectionManager(Settings settings, Transport transport, ThreadPool threadPool) {
        super(settings, transport, threadPool);
    }

    @Override
    public Transport.Connection getConnection(DiscoveryNode node) {
        return new WrappedConnection(super.getConnection(node));
    }

    public void addSendBehavior(TransportAddress transportAddress, MockTransportService.SendRequestBehavior sendBehavior) {
        sendBehaviors.put(transportAddress, sendBehavior);
    }

    public void clearSendBehaviors() {
        sendBehaviors.clear();
    }

    public void clearSendBehavior(TransportAddress transportAddress) {
        MockTransportService.SendRequestBehavior behavior = sendBehaviors.remove(transportAddress);
        if (behavior != null) {
            behavior.clearCallback();
        }
    }

    public Transport.Connection wrapWithBehavior(Transport.Connection connection) {
        return new WrappedConnection(connection);
    }

    private class WrappedConnection implements Transport.Connection {

        private final Transport.Connection connection;

        private WrappedConnection(Transport.Connection connection) {
            this.connection = connection;
        }

        @Override
        public DiscoveryNode getNode() {
            return connection.getNode();
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws IOException, TransportException {
            TransportAddress address = connection.getNode().getAddress();
            MockTransportService.SendRequestBehavior behavior = sendBehaviors.getOrDefault(address, defaultSendRequest);
            if (behavior == null) {
                connection.sendRequest(requestId, action, request, options);
            } else {
                // TODO: Connections maybe not the same?
                behavior.sendRequest(this, requestId, action, request, options);
            }
        }

        @Override
        public boolean sendPing() {
            return connection.sendPing();
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
            connection.addCloseListener(listener);
        }


        @Override
        public boolean isClosed() {
            return connection.isClosed();
        }

        @Override
        public Version getVersion() {
            return getNode().getVersion();
        }

        @Override
        public Object getCacheKey() {
            return connection.getCacheKey();
        }

        @Override
        public void close() throws IOException {
            connection.close();
        }

        @Override
        public boolean equals(Object obj) {
            return connection.equals(obj);
        }

        @Override
        public int hashCode() {
            return connection.hashCode();
        }
    }
}
