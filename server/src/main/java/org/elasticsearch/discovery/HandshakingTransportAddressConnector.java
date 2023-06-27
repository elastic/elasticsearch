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
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.TransportRequestOptions.Type;
import org.elasticsearch.transport.TransportService;

import java.util.Locale;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.core.Strings.format;

public class HandshakingTransportAddressConnector implements TransportAddressConnector {

    private static final Logger logger = LogManager.getLogger(HandshakingTransportAddressConnector.class);

    // connection timeout for probes
    public static final Setting<TimeValue> PROBE_CONNECT_TIMEOUT_SETTING = Setting.timeSetting(
        "discovery.probe.connect_timeout",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope
    );
    // handshake timeout for probes
    public static final Setting<TimeValue> PROBE_HANDSHAKE_TIMEOUT_SETTING = Setting.timeSetting(
        "discovery.probe.handshake_timeout",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope
    );

    private final TransportService transportService;

    private final ConnectionProfile handshakeConnectionProfile;

    public HandshakingTransportAddressConnector(Settings settings, TransportService transportService) {
        this.transportService = transportService;
        handshakeConnectionProfile = ConnectionProfile.buildSingleChannelProfile(
            Type.REG,
            PROBE_CONNECT_TIMEOUT_SETTING.get(settings),
            PROBE_HANDSHAKE_TIMEOUT_SETTING.get(settings),
            TimeValue.MINUS_ONE,
            null,
            null
        );
    }

    @Override
    public void connectToRemoteMasterNode(TransportAddress transportAddress, ActionListener<ProbeConnectionResult> listener) {
        try {

            // We could skip this if the transportService were already connected to the given address, but the savings would be minimal so
            // we open a new connection anyway.

            logger.trace("[{}] opening probe connection", transportAddress);
            transportService.openConnection(
                new DiscoveryNode(
                    "",
                    transportAddress.toString(),
                    UUIDs.randomBase64UUID(Randomness.get()), // generated deterministically for reproducible tests
                    transportAddress.address().getHostString(),
                    transportAddress.getAddress(),
                    transportAddress,
                    emptyMap(),
                    emptySet(),
                    new VersionInformation(
                        Version.CURRENT.minimumCompatibilityVersion(),
                        IndexVersion.MINIMUM_COMPATIBLE,
                        IndexVersion.CURRENT
                    )
                ),
                handshakeConnectionProfile,
                listener.delegateFailure((l, connection) -> {
                    logger.trace("[{}] opened probe connection", transportAddress);
                    final var probeHandshakeTimeout = handshakeConnectionProfile.getHandshakeTimeout();
                    // use NotifyOnceListener to make sure the following line does not result in onFailure being called when
                    // the connection is closed in the onResponse handler
                    transportService.handshake(connection, probeHandshakeTimeout, ActionListener.notifyOnce(new ActionListener<>() {

                        @Override
                        public void onResponse(DiscoveryNode remoteNode) {
                            try {
                                // success means (amongst other things) that the cluster names match
                                logger.trace("[{}] handshake successful: {}", transportAddress, remoteNode);
                                IOUtils.closeWhileHandlingException(connection);

                                if (remoteNode.equals(transportService.getLocalNode())) {
                                    listener.onFailure(
                                        new ConnectTransportException(
                                            remoteNode,
                                            String.format(
                                                Locale.ROOT,
                                                "successfully discovered local node %s at [%s]",
                                                remoteNode.descriptionWithoutAttributes(),
                                                transportAddress
                                            )
                                        )
                                    );
                                } else if (remoteNode.isMasterNode() == false) {
                                    listener.onFailure(
                                        new ConnectTransportException(
                                            remoteNode,
                                            String.format(
                                                Locale.ROOT,
                                                """
                                                    successfully discovered master-ineligible node %s at [%s]; to suppress this message, \
                                                    remove address [%s] from your discovery configuration or ensure that traffic to this \
                                                    address is routed only to master-eligible nodes""",
                                                remoteNode.descriptionWithoutAttributes(),
                                                transportAddress,
                                                transportAddress
                                            )
                                        )
                                    );
                                } else {
                                    transportService.connectToNode(remoteNode, new ActionListener<>() {
                                        @Override
                                        public void onResponse(Releasable connectionReleasable) {
                                            logger.trace("[{}] completed full connection with [{}]", transportAddress, remoteNode);
                                            listener.onResponse(new ProbeConnectionResult(remoteNode, connectionReleasable));
                                        }

                                        @Override
                                        public void onFailure(Exception e) {
                                            // we opened a connection and successfully performed a handshake, so we're definitely
                                            // talking to a master-eligible node with a matching cluster name and a good version, but
                                            // the attempt to open a full connection to its publish address failed; a common reason is
                                            // that the remote node is listening on 0.0.0.0 but has made an inappropriate choice for its
                                            // publish address.
                                            logger.warn(
                                                () -> format(
                                                    "completed handshake with [%s] at [%s] but followup connection to [%s] failed",
                                                    remoteNode.descriptionWithoutAttributes(),
                                                    transportAddress,
                                                    remoteNode.getAddress()
                                                ),
                                                e
                                            );
                                            listener.onFailure(e);
                                        }
                                    });
                                }
                            } catch (Exception e) {
                                listener.onFailure(e);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // we opened a connection and successfully performed a low-level handshake, so we were definitely
                            // talking to an Elasticsearch node, but the high-level handshake failed indicating some kind of
                            // mismatched configurations (e.g. cluster name) that the user should address
                            logger.warn(() -> "handshake to [" + transportAddress + "] failed", e);
                            IOUtils.closeWhileHandlingException(connection);
                            listener.onFailure(e);
                        }

                    }));

                })
            );

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
