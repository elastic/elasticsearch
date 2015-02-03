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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * A mock transport service that allows to simulate different network topology failures.
 */
public class MockTransportService extends TransportService {

    private final Transport original;

    @Inject
    public MockTransportService(Settings settings, Transport transport, ThreadPool threadPool) {
        super(settings, new LookupTestTransport(transport), threadPool);
        this.original = transport;

    }

    /**
     * Clears all the registered rules.
     */
    public void clearAllRules() {
        transport().transports.clear();
    }

    /**
     * Clears the rule associated with the provided node.
     */
    public void clearRule(DiscoveryNode node) {
        transport().transports.remove(node.getAddress());
    }

    /**
     * Returns the original Transport service wrapped by this mock transport service.
     */
    public Transport original() {
        return original;
    }

    /**
     * Adds a rule that will cause every send request to fail, and each new connect since the rule
     * is added to fail as well.
     */
    public void addFailToSendNoConnectRule(DiscoveryNode node) {
        addDelegate(node, new DelegateTransport(original) {
            @Override
            public void connectToNode(DiscoveryNode node) throws ConnectTransportException {
                throw new ConnectTransportException(node, "DISCONNECT: simulated");
            }

            @Override
            public void connectToNodeLight(DiscoveryNode node) throws ConnectTransportException {
                throw new ConnectTransportException(node, "DISCONNECT: simulated");
            }

            @Override
            public void sendRequest(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
                throw new ConnectTransportException(node, "DISCONNECT: simulated");
            }
        });
    }

    /**
     * Adds a rule that will cause matching operations to throw ConnectTransportExceptions
     */
    public void addFailToSendNoConnectRule(DiscoveryNode node, final String... blockedActions) {
        addFailToSendNoConnectRule(node, new HashSet<>(Arrays.asList(blockedActions)));
    }

    /**
     * Adds a rule that will cause matching operations to throw ConnectTransportExceptions
     */
    public void addFailToSendNoConnectRule(DiscoveryNode node, final Set<String> blockedActions) {

        addDelegate(node, new DelegateTransport(original) {
            @Override
            public void connectToNode(DiscoveryNode node) throws ConnectTransportException {
                original.connectToNode(node);
            }

            @Override
            public void connectToNodeLight(DiscoveryNode node) throws ConnectTransportException {
                original.connectToNodeLight(node);
            }

            @Override
            public void sendRequest(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
                if (blockedActions.contains(action)) {
                    logger.info("--> preventing {} request", action);
                    throw new ConnectTransportException(node, "DISCONNECT: prevented " + action + " request");
                }
                original.sendRequest(node, requestId, action, request, options);
            }
        });
    }

    /**
     * Adds a rule that will cause ignores each send request, simulating an unresponsive node
     * and failing to connect once the rule was added.
     */
    public void addUnresponsiveRule(DiscoveryNode node) {
        addDelegate(node, new DelegateTransport(original) {
            @Override
            public void connectToNode(DiscoveryNode node) throws ConnectTransportException {
                throw new ConnectTransportException(node, "UNRESPONSIVE: simulated");
            }

            @Override
            public void connectToNodeLight(DiscoveryNode node) throws ConnectTransportException {
                throw new ConnectTransportException(node, "UNRESPONSIVE: simulated");
            }

            @Override
            public void sendRequest(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
                // don't send anything, the receiving node is unresponsive
            }
        });
    }

    /**
     * Adds a rule that will cause ignores each send request, simulating an unresponsive node
     * and failing to connect once the rule was added.
     *
     * @param duration the amount of time to delay sending and connecting.
     */
    public void addUnresponsiveRule(DiscoveryNode node, final TimeValue duration) {
        final long startTime = System.currentTimeMillis();

        addDelegate(node, new DelegateTransport(original) {

            TimeValue getDelay() {
                return new TimeValue(duration.millis() - (System.currentTimeMillis() - startTime));
            }

            @Override
            public void connectToNode(DiscoveryNode node) throws ConnectTransportException {
                TimeValue delay = getDelay();
                if (delay.millis() <= 0) {
                    original.connectToNode(node);
                    return;
                }

                // TODO: Replace with proper setting
                TimeValue connectingTimeout = NetworkService.TcpSettings.TCP_DEFAULT_CONNECT_TIMEOUT;
                try {
                    if (delay.millis() < connectingTimeout.millis()) {
                        Thread.sleep(delay.millis());
                        original.connectToNode(node);
                    } else {
                        Thread.sleep(connectingTimeout.millis());
                        throw new ConnectTransportException(node, "UNRESPONSIVE: simulated");
                    }
                } catch (InterruptedException e) {
                    throw new ConnectTransportException(node, "UNRESPONSIVE: interrupted while sleeping", e);
                }
            }

            @Override
            public void connectToNodeLight(DiscoveryNode node) throws ConnectTransportException {
                TimeValue delay = getDelay();
                if (delay.millis() <= 0) {
                    original.connectToNodeLight(node);
                    return;
                }

                // TODO: Replace with proper setting
                TimeValue connectingTimeout = NetworkService.TcpSettings.TCP_DEFAULT_CONNECT_TIMEOUT;
                try {
                    if (delay.millis() < connectingTimeout.millis()) {
                        Thread.sleep(delay.millis());
                        original.connectToNodeLight(node);
                    } else {
                        Thread.sleep(connectingTimeout.millis());
                        throw new ConnectTransportException(node, "UNRESPONSIVE: simulated");
                    }
                } catch (InterruptedException e) {
                    throw new ConnectTransportException(node, "UNRESPONSIVE: interrupted while sleeping", e);
                }
            }

            @Override
            public void sendRequest(final DiscoveryNode node, final long requestId, final String action, TransportRequest request, final TransportRequestOptions options) throws IOException, TransportException {
                // delayed sending - even if larger then the request timeout to simulated a potential late response from target node

                TimeValue delay = getDelay();
                if (delay.millis() <= 0) {
                    original.sendRequest(node, requestId, action, request, options);
                    return;
                }

                // poor mans request cloning...
                TransportRequestHandler handler = MockTransportService.this.getHandler(action);
                BytesStreamOutput bStream = new BytesStreamOutput();
                request.writeTo(bStream);
                final TransportRequest clonedRequest = handler.newInstance();
                clonedRequest.readFrom(new BytesStreamInput(bStream.bytes()));

                threadPool.schedule(delay, ThreadPool.Names.GENERIC, new AbstractRunnable() {
                    @Override
                    public void onFailure(Throwable e) {
                        logger.debug("failed to send delayed request", e);
                    }

                    @Override
                    protected void doRun() throws IOException {
                        original.sendRequest(node, requestId, action, clonedRequest, options);
                    }
                });
            }
        });
    }

    /**
     * Adds a new delegate transport that is used for communication with the given node.
     *
     * @return <tt>true</tt> iff no other delegate was registered for this node before, otherwise <tt>false</tt>
     */
    public boolean addDelegate(DiscoveryNode node, DelegateTransport transport) {
        return transport().transports.put(node.getAddress(), transport) == null;
    }

    private LookupTestTransport transport() {
        return (LookupTestTransport) transport;
    }

    /**
     * A lookup transport that has a list of potential Transport implementations to delegate to for node operations,
     * if none is registered, then the default one is used.
     */
    private static class LookupTestTransport extends DelegateTransport {

        final ConcurrentMap<TransportAddress, Transport> transports = ConcurrentCollections.newConcurrentMap();

        LookupTestTransport(Transport transport) {
            super(transport);
        }

        private Transport getTransport(DiscoveryNode node) {
            Transport transport = transports.get(node.getAddress());
            if (transport != null) {
                return transport;
            }
            return this.transport;
        }

        @Override
        public boolean nodeConnected(DiscoveryNode node) {
            return getTransport(node).nodeConnected(node);
        }

        @Override
        public void connectToNode(DiscoveryNode node) throws ConnectTransportException {
            getTransport(node).connectToNode(node);
        }

        @Override
        public void connectToNodeLight(DiscoveryNode node) throws ConnectTransportException {
            getTransport(node).connectToNodeLight(node);
        }

        @Override
        public void disconnectFromNode(DiscoveryNode node) {
            getTransport(node).disconnectFromNode(node);
        }

        @Override
        public void sendRequest(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
            getTransport(node).sendRequest(node, requestId, action, request, options);
        }
    }

    /**
     * A pure delegate transport.
     * Can be extracted to a common class if needed in other places in the codebase.
     */
    public static class DelegateTransport implements Transport {

        protected final Transport transport;


        public DelegateTransport(Transport transport) {
            this.transport = transport;
        }

        @Override
        public void transportServiceAdapter(TransportServiceAdapter service) {
            transport.transportServiceAdapter(service);
        }

        @Override
        public BoundTransportAddress boundAddress() {
            return transport.boundAddress();
        }

        @Override
        public TransportAddress[] addressesFromString(String address) throws Exception {
            return transport.addressesFromString(address);
        }

        @Override
        public boolean addressSupported(Class<? extends TransportAddress> address) {
            return transport.addressSupported(address);
        }

        @Override
        public boolean nodeConnected(DiscoveryNode node) {
            return transport.nodeConnected(node);
        }

        @Override
        public void connectToNode(DiscoveryNode node) throws ConnectTransportException {
            transport.connectToNode(node);
        }

        @Override
        public void connectToNodeLight(DiscoveryNode node) throws ConnectTransportException {
            transport.connectToNodeLight(node);
        }

        @Override
        public void disconnectFromNode(DiscoveryNode node) {
            transport.disconnectFromNode(node);
        }

        @Override
        public void sendRequest(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
            transport.sendRequest(node, requestId, action, request, options);
        }

        @Override
        public long serverOpen() {
            return transport.serverOpen();
        }

        @Override
        public Lifecycle.State lifecycleState() {
            return transport.lifecycleState();
        }

        @Override
        public void addLifecycleListener(LifecycleListener listener) {
            transport.addLifecycleListener(listener);
        }

        @Override
        public void removeLifecycleListener(LifecycleListener listener) {
            transport.removeLifecycleListener(listener);
        }

        @Override
        public Transport start() throws ElasticsearchException {
            transport.start();
            return this;
        }

        @Override
        public Transport stop() throws ElasticsearchException {
            transport.stop();
            return this;
        }

        @Override
        public void close() throws ElasticsearchException {
            transport.close();
        }
    }
}
