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

package org.elasticsearch.cluster.node;

import com.google.common.collect.ImmutableMap;

import org.elasticsearch.Version;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.*;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.transport.TransportAddressSerializers;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.transport.TransportAddressSerializers.addressToStream;

/**
 * A discovery node represents a node that is part of the cluster.
 */
public class DiscoveryNode implements Streamable, ToXContent {

    /**
     * Minimum version of a node to communicate with. This version corresponds to the minimum compatibility version
     * of the current elasticsearch major version.
     */
    public static final Version MINIMUM_DISCOVERY_NODE_VERSION = Version.CURRENT.minimumCompatibilityVersion();

    public static boolean localNode(Settings settings) {
        if (settings.get("node.local") != null) {
            return settings.getAsBoolean("node.local", false);
        }
        if (settings.get("node.mode") != null) {
            String nodeMode = settings.get("node.mode");
            if ("local".equals(nodeMode)) {
                return true;
            } else if ("network".equals(nodeMode)) {
                return false;
            } else {
                throw new IllegalArgumentException("unsupported node.mode [" + nodeMode + "]. Should be one of [local, network].");
            }
        }
        return false;
    }

    public static boolean nodeRequiresLocalStorage(Settings settings) {
        return !(settings.getAsBoolean("node.client", false) || (!settings.getAsBoolean("node.data", true) && !settings.getAsBoolean("node.master", true)));
    }

    public static boolean clientNode(Settings settings) {
        String client = settings.get("node.client");
        return Booleans.isExplicitTrue(client);
    }

    public static boolean masterNode(Settings settings) {
        String master = settings.get("node.master");
        if (master == null) {
            return !clientNode(settings);
        }
        return Booleans.isExplicitTrue(master);
    }

    public static boolean dataNode(Settings settings) {
        String data = settings.get("node.data");
        if (data == null) {
            return !clientNode(settings);
        }
        return Booleans.isExplicitTrue(data);
    }

    public static final List<DiscoveryNode> EMPTY_LIST = Collections.emptyList();

    private String nodeName = "";
    private String nodeId;
    private String hostName;
    private String hostAddress;
    private TransportAddress address;
    private ImmutableMap<String, String> attributes;
    private Version version = Version.CURRENT;

    DiscoveryNode() {
    }

    /**
     * Creates a new {@link DiscoveryNode}
     * <p>
     * <b>Note:</b> if the version of the node is unknown {@link #MINIMUM_DISCOVERY_NODE_VERSION} should be used.
     * it corresponds to the minimum version this elasticsearch version can communicate with. If a higher version is used
     * the node might not be able to communicate with the remove node. After initial handshakes node versions will be discovered
     * and updated.
     * </p>
     *
     * @param nodeId  the nodes unique id.
     * @param address the nodes transport address
     * @param version the version of the node.
     */
    public DiscoveryNode(String nodeId, TransportAddress address, Version version) {
        this("", nodeId, address, ImmutableMap.<String, String>of(), version);
    }

    /**
     * Creates a new {@link DiscoveryNode}
     * <p>
     * <b>Note:</b> if the version of the node is unknown {@link #MINIMUM_DISCOVERY_NODE_VERSION} should be used.
     * it corresponds to the minimum version this elasticsearch version can communicate with. If a higher version is used
     * the node might not be able to communicate with the remove node. After initial handshakes node versions will be discovered
     * and updated.
     * </p>
     *
     * @param nodeName   the nodes name
     * @param nodeId     the nodes unique id.
     * @param address    the nodes transport address
     * @param attributes node attributes
     * @param version    the version of the node.
     */
    public DiscoveryNode(String nodeName, String nodeId, TransportAddress address, Map<String, String> attributes, Version version) {
        this(nodeName, nodeId, address.getHost(), address.getAddress(), address, attributes, version);
    }

    /**
     * Creates a new {@link DiscoveryNode}
     * <p>
     * <b>Note:</b> if the version of the node is unknown {@link #MINIMUM_DISCOVERY_NODE_VERSION} should be used.
     * it corresponds to the minimum version this elasticsearch version can communicate with. If a higher version is used
     * the node might not be able to communicate with the remove node. After initial handshakes node versions will be discovered
     * and updated.
     * </p>
     *
     * @param nodeName    the nodes name
     * @param nodeId      the nodes unique id.
     * @param hostName    the nodes hostname
     * @param hostAddress the nodes host address
     * @param address     the nodes transport address
     * @param attributes  node attributes
     * @param version     the version of the node.
     */
    public DiscoveryNode(String nodeName, String nodeId, String hostName, String hostAddress, TransportAddress address, Map<String, String> attributes, Version version) {
        if (nodeName != null) {
            this.nodeName = nodeName.intern();
        }
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            builder.put(entry.getKey().intern(), entry.getValue().intern());
        }
        this.attributes = builder.build();
        this.nodeId = nodeId.intern();
        this.hostName = hostName.intern();
        this.hostAddress = hostAddress.intern();
        this.address = address;
        this.version = version;
    }

    /**
     * Should this node form a connection to the provided node.
     */
    public boolean shouldConnectTo(DiscoveryNode otherNode) {
        if (clientNode() && otherNode.clientNode()) {
            return false;
        }
        return true;
    }

    /**
     * The address that the node can be communicated with.
     */
    public TransportAddress address() {
        return address;
    }

    /**
     * The address that the node can be communicated with.
     */
    public TransportAddress getAddress() {
        return address();
    }

    /**
     * The unique id of the node.
     */
    public String id() {
        return nodeId;
    }

    /**
     * The unique id of the node.
     */
    public String getId() {
        return id();
    }

    /**
     * The name of the node.
     */
    public String name() {
        return this.nodeName;
    }

    /**
     * The name of the node.
     */
    public String getName() {
        return name();
    }

    /**
     * The node attributes.
     */
    public ImmutableMap<String, String> attributes() {
        return this.attributes;
    }

    /**
     * The node attributes.
     */
    public ImmutableMap<String, String> getAttributes() {
        return attributes();
    }

    /**
     * Should this node hold data (shards) or not.
     */
    public boolean dataNode() {
        String data = attributes.get("data");
        if (data == null) {
            return !clientNode();
        }
        return Booleans.parseBooleanExact(data);
    }

    /**
     * Should this node hold data (shards) or not.
     */
    public boolean isDataNode() {
        return dataNode();
    }

    /**
     * Is the node a client node or not.
     */
    public boolean clientNode() {
        String client = attributes.get("client");
        return client != null && Booleans.parseBooleanExact(client);
    }

    public boolean isClientNode() {
        return clientNode();
    }

    /**
     * Can this node become master or not.
     */
    public boolean masterNode() {
        String master = attributes.get("master");
        if (master == null) {
            return !clientNode();
        }
        return Booleans.parseBooleanExact(master);
    }

    /**
     * Can this node become master or not.
     */
    public boolean isMasterNode() {
        return masterNode();
    }

    public Version version() {
        return this.version;
    }

    public String getHostName() {
        return this.hostName;
    }

    public String getHostAddress() {
        return this.hostAddress;
    }

    public Version getVersion() {
        return this.version;
    }

    public static DiscoveryNode readNode(StreamInput in) throws IOException {
        DiscoveryNode node = new DiscoveryNode();
        node.readFrom(in);
        return node;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        nodeName = in.readString().intern();
        nodeId = in.readString().intern();
        hostName = in.readString().intern();
        hostAddress = in.readString().intern();
        address = TransportAddressSerializers.addressFromStream(in);
        int size = in.readVInt();
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (int i = 0; i < size; i++) {
            builder.put(in.readString().intern(), in.readString().intern());
        }
        attributes = builder.build();
        version = Version.readVersion(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeName);
        out.writeString(nodeId);
        out.writeString(hostName);
        out.writeString(hostAddress);
        addressToStream(out, address);
        out.writeVInt(attributes.size());
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
        Version.writeVersion(version, out);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DiscoveryNode)) {
            return false;
        }

        DiscoveryNode other = (DiscoveryNode) obj;
        return this.nodeId.equals(other.nodeId);
    }

    @Override
    public int hashCode() {
        return nodeId.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (nodeName.length() > 0) {
            sb.append('{').append(nodeName).append('}');
        }
        if (nodeId != null) {
            sb.append('{').append(nodeId).append('}');
        }
        if (Strings.hasLength(hostName)) {
            sb.append('{').append(hostName).append('}');
        }
        if (address != null) {
            sb.append('{').append(address).append('}');
        }
        if (!attributes.isEmpty()) {
            sb.append(attributes);
        }
        return sb.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(id(), XContentBuilder.FieldCaseConversion.NONE);
        builder.field("name", name());
        builder.field("transport_address", address().toString());

        builder.startObject("attributes");
        for (Map.Entry<String, String> attr : attributes().entrySet()) {
            builder.field(attr.getKey(), attr.getValue());
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }
}
