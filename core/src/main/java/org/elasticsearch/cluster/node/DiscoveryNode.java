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

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.transport.TransportAddressSerializers;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.common.transport.TransportAddressSerializers.addressToStream;

/**
 * A discovery node represents a node that is part of the cluster.
 */
public class DiscoveryNode implements Writeable, ToXContent {

    public static boolean isLocalNode(Settings settings) {
        if (Node.NODE_LOCAL_SETTING.exists(settings)) {
            return Node.NODE_LOCAL_SETTING.get(settings);
        }
        if (Node.NODE_MODE_SETTING.exists(settings)) {
            String nodeMode = Node.NODE_MODE_SETTING.get(settings);
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
        return Node.NODE_DATA_SETTING.get(settings) || Node.NODE_MASTER_SETTING.get(settings);
    }

    public static boolean isMasterNode(Settings settings) {
        return Node.NODE_MASTER_SETTING.get(settings);
    }

    public static boolean isDataNode(Settings settings) {
        return Node.NODE_DATA_SETTING.get(settings);
    }

    public static boolean isIngestNode(Settings settings) {
        return Node.NODE_INGEST_SETTING.get(settings);
    }

    private final String nodeName;
    private final String nodeId;
    private final String hostName;
    private final String hostAddress;
    private final TransportAddress address;
    private final Map<String, String> attributes;
    private final Version version;
    private final Set<Role> roles;

    /**
     * Creates a new {@link DiscoveryNode}
     * <p>
     * <b>Note:</b> if the version of the node is unknown {@link Version#minimumCompatibilityVersion()} should be used for the current
     * version. it corresponds to the minimum version this elasticsearch version can communicate with. If a higher version is used
     * the node might not be able to communicate with the remove node. After initial handshakes node versions will be discovered
     * and updated.
     * </p>
     *
     * @param nodeId     the nodes unique id.
     * @param address    the nodes transport address
     * @param attributes node attributes
     * @param roles      node roles
     * @param version    the version of the node.
     */
    public DiscoveryNode(String nodeId, TransportAddress address, Map<String, String> attributes, Set<Role> roles, Version version) {
        this("", nodeId, address.getHost(), address.getAddress(), address, attributes, roles, version);
    }

    /**
     * Creates a new {@link DiscoveryNode}
     * <p>
     * <b>Note:</b> if the version of the node is unknown {@link Version#minimumCompatibilityVersion()} should be used for the current
     * version. it corresponds to the minimum version this elasticsearch version can communicate with. If a higher version is used
     * the node might not be able to communicate with the remove node. After initial handshakes node versions will be discovered
     * and updated.
     * </p>
     *
     * @param nodeName   the nodes name
     * @param nodeId     the nodes unique id.
     * @param address    the nodes transport address
     * @param attributes node attributes
     * @param roles      node roles
     * @param version    the version of the node.
     */
    public DiscoveryNode(String nodeName, String nodeId, TransportAddress address, Map<String, String> attributes,
                         Set<Role> roles, Version version) {
        this(nodeName, nodeId, address.getHost(), address.getAddress(), address, attributes, roles, version);
    }

    /**
     * Creates a new {@link DiscoveryNode}.
     * <p>
     * <b>Note:</b> if the version of the node is unknown {@link Version#minimumCompatibilityVersion()} should be used for the current
     * version. it corresponds to the minimum version this elasticsearch version can communicate with. If a higher version is used
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
     * @param roles       node roles
     * @param version     the version of the node.
     */
    public DiscoveryNode(String nodeName, String nodeId, String hostName, String hostAddress, TransportAddress address,
                         Map<String, String> attributes, Set<Role> roles, Version version) {
        if (nodeName != null) {
            this.nodeName = nodeName.intern();
        } else {
            this.nodeName = "";
        }
        this.nodeId = nodeId.intern();
        this.hostName = hostName.intern();
        this.hostAddress = hostAddress.intern();
        this.address = address;
        if (version == null) {
            this.version = Version.CURRENT;
        } else {
            this.version = version;
        }
        this.attributes = Collections.unmodifiableMap(attributes);
        //verify that no node roles are being provided as attributes
        Predicate<Map<String, String>> predicate =  (attrs) -> {
            for (Role role : Role.values()) {
                assert attrs.containsKey(role.getRoleName()) == false;
            }
            return true;
        };
        assert predicate.test(attributes);
        Set<Role> rolesSet = EnumSet.noneOf(Role.class);
        rolesSet.addAll(roles);
        this.roles = Collections.unmodifiableSet(rolesSet);
    }

    /**
     * Creates a new {@link DiscoveryNode} by reading from the stream provided as argument
     * @param in the stream
     * @throws IOException if there is an error while reading from the stream
     */
    public DiscoveryNode(StreamInput in) throws IOException {
        this.nodeName = in.readString().intern();
        this.nodeId = in.readString().intern();
        this.hostName = in.readString().intern();
        this.hostAddress = in.readString().intern();
        this.address = TransportAddressSerializers.addressFromStream(in);
        int size = in.readVInt();
        this.attributes = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            this.attributes.put(in.readString(), in.readString());
        }
        int rolesSize = in.readVInt();
        this.roles = EnumSet.noneOf(Role.class);
        for (int i = 0; i < rolesSize; i++) {
            int ordinal = in.readVInt();
            if (ordinal < 0 || ordinal >= Role.values().length) {
                throw new IOException("Unknown Role ordinal [" + ordinal + "]");
            }
            this.roles.add(Role.values()[ordinal]);
        }
        this.version = Version.readVersion(in);
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
        out.writeVInt(roles.size());
        for (Role role : roles) {
            out.writeVInt(role.ordinal());
        }
        Version.writeVersion(version, out);
    }

    /**
     * The address that the node can be communicated with.
     */
    public TransportAddress getAddress() {
        return address;
    }

    /**
     * The unique id of the node.
     */
    public String getId() {
        return nodeId;
    }

    /**
     * The name of the node.
     */
    public String getName() {
        return this.nodeName;
    }

    /**
     * The node attributes.
     */
    public Map<String, String> getAttributes() {
        return this.attributes;
    }

    /**
     * Should this node hold data (shards) or not.
     */
    public boolean isDataNode() {
        return roles.contains(Role.DATA);
    }

    /**
     * Can this node become master or not.
     */
    public boolean isMasterNode() {
        return roles.contains(Role.MASTER);
    }

    /**
     * Returns a boolean that tells whether this an ingest node or not
     */
    public boolean isIngestNode() {
        return roles.contains(Role.INGEST);
    }

    /**
     * Returns a set of all the roles that the node fulfills.
     * If the node doesn't have any specific role, the set is returned empty, which means that the node is a coordinating only node.
     */
    public Set<Role> getRoles() {
        return roles;
    }

    public Version getVersion() {
        return this.version;
    }

    public String getHostName() {
        return this.hostName;
    }

    public String getHostAddress() {
        return this.hostAddress;
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
        builder.startObject(getId());
        builder.field("name", getName());
        builder.field("transport_address", getAddress().toString());

        builder.startObject("attributes");
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }

    /**
     * Enum that holds all the possible roles that that a node can fulfill in a cluster.
     * Each role has its name and a corresponding abbreviation used by cat apis.
     */
    public enum Role {
        MASTER("master", "m"),
        DATA("data", "d"),
        INGEST("ingest", "i");

        private final String roleName;
        private final String abbreviation;

        Role(String roleName, String abbreviation) {
            this.roleName = roleName;
            this.abbreviation = abbreviation;
        }

        public String getRoleName() {
            return roleName;
        }

        public String getAbbreviation() {
            return abbreviation;
        }
    }
}
