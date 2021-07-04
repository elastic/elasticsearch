/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.NotifyOnceListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.discovery.PeerFinder.TransportAddressConnector;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.TransportRequestOptions.Type;
import org.elasticsearch.transport.TransportService;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class HandshakingTransportAddressConnector implements TransportAddressConnector {

    private static final Logger logger = LogManager.getLogger(HandshakingTransportAddressConnector.class);

    // connection timeout for probes
    public static final Setting<TimeValue> PROBE_CONNECT_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.probe.connect_timeout",
            TimeValue.timeValueSeconds(30), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);
    // handshake timeout for probes
    public static final Setting<TimeValue> PROBE_HANDSHAKE_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.probe.handshake_timeout",
            TimeValue.timeValueSeconds(30), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    private final TransportService transportService;
    private final TimeValue probeConnectTimeout;
    private final TimeValue probeHandshakeTimeout;

    public HandshakingTransportAddressConnector(Settings settings, TransportService transportService) {
        this.transportService = transportService;
        probeConnectTimeout = PROBE_CONNECT_TIMEOUT_SETTING.get(settings);
        probeHandshakeTimeout = PROBE_HANDSHAKE_TIMEOUT_SETTING.get(settings);
    }

    @Override
    public void connectToRemoteMasterNode(TransportAddress transportAddress, ActionListener<DiscoveryNode> listener) {
        transportService.getThreadPool().generic().execute(new ActionRunnable<>(listener) {
            private final AbstractRunnable thisConnectionAttempt = this;

            @Override
            protected void doRun() {
                // We could skip this if the transportService were already connected to the given address, but the savings would be minimal
                // so we open a new connection anyway.

                final DiscoveryNode targetNode = new DiscoveryNode("", transportAddress.toString(),
                    UUIDs.randomBase64UUID(Randomness.get()), // generated deterministically for reproducible tests
                    transportAddress.address().getHostString(), transportAddress.getAddress(), transportAddress, emptyMap(),
                    emptySet(), Version.CURRENT.minimumCompatibilityVersion());

                logger.trace("[{}] opening probe connection", thisConnectionAttempt);
                transportService.openConnection(targetNode,
                    ConnectionProfile.buildSingleChannelProfile(Type.REG, probeConnectTimeout, probeHandshakeTimeout,
                        TimeValue.MINUS_ONE, null, null), listener.delegateFailure((l, connection) -> {
                        logger.trace("[{}] opened probe connection", thisConnectionAttempt);

                        // use NotifyOnceListener to make sure the following line does not result in onFailure being called when
                        // the connection is closed in the onResponse handler
                        transportService.handshake(connection, probeHandshakeTimeout, new NotifyOnceListener<>() {

                            @Override
                            protected void innerOnResponse(DiscoveryNode remoteNode) {
                                try {
                                    // success means (amongst other things) that the cluster names match
                                    logger.trace("[{}] handshake successful: {}", thisConnectionAttempt, remoteNode);
                                    IOUtils.closeWhileHandlingException(connection);

                                    if (remoteNode.equals(transportService.getLocalNode())) {
                                        listener.onFailure(new ConnectTransportException(remoteNode, "local node found"));
                                    } else if (remoteNode.isMasterNode() == false) {
                                        listener.onFailure(new ConnectTransportException(remoteNode, "non-master-eligible node found"));
                                    } else {
                                        transportService.connectToNode(remoteNode, new ActionListener<>() {
                                            @Override
                                            public void onResponse(Void ignored) {
                                                logger.trace("[{}] completed full connection with [{}]", thisConnectionAttempt, remoteNode);
                                                listener.onResponse(remoteNode);
                                            }

                                            @Override
                                            public void onFailure(Exception e) {
                                                // we opened a connection and successfully performed a handshake, so we're definitely
                                                // talking to a master-eligible node with a matching cluster name and a good version, but
                                                // the attempt to open a full connection to its publish address failed; a common reason is
                                                // that the remote node is listening on 0.0.0.0 but has made an inappropriate choice for its
                                                // publish address.
                                                logger.warn(new ParameterizedMessage(
                                                    "[{}] completed handshake with [{}] but followup connection failed",
                                                    thisConnectionAttempt, remoteNode), e);
                                                listener.onFailure(e);
                                            }
                                        });
                                    }
                                } catch (Exception e) {
                                    listener.onFailure(e);
                                }
                            }

                            @Override
                            protected void innerOnFailure(Exception e) {
                                // we opened a connection and successfully performed a low-level handshake, so we were definitely
                                // talking to an Elasticsearch node, but the high-level handshake failed indicating some kind of
                                // mismatched configurations (e.g. cluster name) that the user should address
                                logger.warn(new ParameterizedMessage("handshake failed for [{}]", thisConnectionAttempt), e);
                                IOUtils.closeWhileHandlingException(connection);
                                listener.onFailure(e);
                            }

                        });

                    }));

            }

            @Override
            public String toString() {
                return "connectToRemoteMasterNode[" + transportAddress + "]";
            }
        });
    }
}
