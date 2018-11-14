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

package org.elasticsearch.discovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.discovery.PeerFinder.TransportAddressConnector;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.Transport.Connection;
import org.elasticsearch.transport.TransportRequestOptions.Type;
import org.elasticsearch.transport.TransportService;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class HandshakingTransportAddressConnector implements TransportAddressConnector {

    private static final Logger logger = LogManager.getLogger(HandshakingTransportAddressConnector.class);

    // connection timeout for probes
    public static final Setting<TimeValue> PROBE_CONNECT_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.probe.connect_timeout",
            TimeValue.timeValueMillis(3000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);
    // handshake timeout for probes
    public static final Setting<TimeValue> PROBE_HANDSHAKE_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.probe.handshake_timeout",
            TimeValue.timeValueMillis(1000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

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
        transportService.getThreadPool().generic().execute(new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {

                // TODO if transportService is already connected to this address then skip the handshaking

                final DiscoveryNode targetNode = new DiscoveryNode("", transportAddress.toString(),
                    UUIDs.randomBase64UUID(Randomness.get()), // generated deterministically for reproducible tests
                    transportAddress.address().getHostString(), transportAddress.getAddress(), transportAddress, emptyMap(),
                    emptySet(), Version.CURRENT.minimumCompatibilityVersion());

                logger.trace("[{}] opening probe connection", this);
                final Connection connection = transportService.openConnection(targetNode,
                    ConnectionProfile.buildSingleChannelProfile(Type.REG, probeConnectTimeout, probeHandshakeTimeout));
                logger.trace("[{}] opened probe connection", this);

                final DiscoveryNode remoteNode;
                try {
                    remoteNode = transportService.handshake(connection, probeHandshakeTimeout.millis());
                    // success means (amongst other things) that the cluster names match
                    logger.trace("[{}] handshake successful: {}", this, remoteNode);
                } finally {
                    IOUtils.closeWhileHandlingException(connection);
                }

                if (remoteNode.equals(transportService.getLocalNode())) {
                    // TODO cache this result for some time? forever?
                    listener.onFailure(new ConnectTransportException(remoteNode, "local node found"));
                } else if (remoteNode.isMasterNode() == false) {
                    // TODO cache this result for some time?
                    listener.onFailure(new ConnectTransportException(remoteNode, "non-master-eligible node found"));
                } else {
                    transportService.connectToNode(remoteNode);
                    logger.trace("[{}] full connection successful: {}", this, remoteNode);
                    listener.onResponse(remoteNode);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public String toString() {
                return "connectToRemoteMasterNode[" + transportAddress + "]";
            }
        });
    }
}
