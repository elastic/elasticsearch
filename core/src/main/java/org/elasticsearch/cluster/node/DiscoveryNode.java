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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.transport.TransportAddressSerializers;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.transport.TransportAddressSerializers.addressToStream;

/**
 * A discovery node represents a node that is part of the cluster.
 */
public class DiscoveryNode implements Writeable<DiscoveryNode>, ToXContent {

    private static final DiscoveryNode PROTOTYPE = new DiscoveryNode("prototype", DummyTransportAddress.INSTANCE, Version.CURRENT);

    public static boolean localNode(Settings settings) {
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

    public static boolean masterNode(Settings settings) {
        return Node.NODE_MASTER_SETTING.get(settings);
    }

    public static boolean dataNode(Settings settings) {
        return Node.NODE_DATA_SETTING.get(settings);
    }

    public static boolean ingestNode(Settings settings) {
        return Node.NODE_INGEST_SETTING.get(settings);
    }

    public static final List<DiscoveryNode> EMPTY_LIST = Collections.emptyList();

    private final String nodeName;
    private final String nodeId;
    private final String hostName;
    private final String hostAddress;
    private final TransportAddress address;
    private final ImmutableOpenMap<String, String> attributes;
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
     * @param nodeId  the nodes unique id.
     * @param address the nodes transport address
     * @param version the version of the node.
     */
    public DiscoveryNode(String nodeId, TransportAddress address, Version version) {
        this("", nodeId, address, Collections.emptyMap(), version);
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
     * @param version    the version of the node.
     */
    public DiscoveryNode(String nodeName, String nodeId, TransportAddress address, Map<String, String> attributes, Version version) {
        this(nodeName, nodeId, address.getHost(), address.getAddress(), address, attributes, version);
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
     * @param version     the version of the node.
     */
    public DiscoveryNode(String nodeName, String nodeId, String hostName, String hostAddress, TransportAddress address,
                         Map<String, String> attributes, Version version) {
        this(nodeName, nodeId, hostName, hostAddress, address, copyAttributes(attributes), version);
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
     * @param version     the version of the node.
     */
    public DiscoveryNode(String nodeName, String nodeId, String hostName, String hostAddress, TransportAddress address,
                         ImmutableOpenMap<String, String> attributes, Version version) {
        this(nodeName, nodeId, hostName, hostAddress, address, copyAttributes(attributes), version);
    }

    private DiscoveryNode(String nodeName, String nodeId, String hostName, String hostAddress, TransportAddress address,
                          ImmutableOpenMap.Builder<String, String> attributesBuilder, Version version) {
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
        this.attributes = attributesBuilder.build();
        this.roles = resolveRoles(this.attributes);
    }

    private static ImmutableOpenMap.Builder<String, String> copyAttributes(ImmutableOpenMap<String, String> attributes) {
        //we could really use copyOf and get rid of this method but we call String#intern while copying...
        ImmutableOpenMap.Builder<String, String> builder = ImmutableOpenMap.builder();
        for (ObjectObjectCursor<String, String> entry : attributes) {
            builder.put(entry.key.intern(), entry.value.intern());
        }
        return builder;
    }

    private static ImmutableOpenMap.Builder<String, String> copyAttributes(Map<String, String> attributes) {
        ImmutableOpenMap.Builder<String, String> builder = ImmutableOpenMap.builder();
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            builder.put(entry.getKey().intern(), entry.getValue().intern());
        }
        return builder;
    }

    private static Set<Role> resolveRoles(ImmutableOpenMap<String, String> attributes) {
        Set<Role> roles = new HashSet<>();
        for (Role role : Role.values()) {
            String roleAttribute = attributes.get(role.getRoleName());
            //all existing roles default to true
            if (roleAttribute == null || Booleans.parseBooleanExact(roleAttribute)) {
                roles.add(role);
            }
        }
        return roles;
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
    public ImmutableOpenMap<String, String> attributes() {
        return this.attributes;
    }

    /**
     * The node attributes.
     */
    public ImmutableOpenMap<String, String> getAttributes() {
        return attributes();
    }

    /**
     * Should this node hold data (shards) or not.
     */
    public boolean dataNode() {
        return roles.contains(Role.DATA);
    }

    /**
     * Should this node hold data (shards) or not.
     */
    public boolean isDataNode() {
        return dataNode();
    }

    /**
     * Can this node become master or not.
     */
    public boolean masterNode() {
        return roles.contains(Role.MASTER);
    }

    /**
     * Can this node become master or not.
     */
    public boolean isMasterNode() {
        return masterNode();
    }

    /**
     * Returns a boolean that tells whether this an ingest node or not
     */
    public boolean isIngestNode() {
        return roles.contains(Role.INGEST);
    }

    public Set<Role> getRoles() {
        return roles;
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
        return PROTOTYPE.readFrom(in);
    }

    @Override
    public DiscoveryNode readFrom(StreamInput in) throws IOException {
        String nodeName = in.readString().intern();
        String nodeId = in.readString().intern();
        String hostName = in.readString().intern();
        String hostAddress = in.readString().intern();
        TransportAddress address = TransportAddressSerializers.addressFromStream(in);
        int size = in.readVInt();
        ImmutableOpenMap.Builder<String, String> attributesBuilder = ImmutableOpenMap.builder(size);
        for (int i = 0; i < size; i++) {
            attributesBuilder.put(in.readString().intern(), in.readString().intern());
        }
        ImmutableOpenMap<String, String> attributes = attributesBuilder.build();
        Version version = Version.readVersion(in);
        return new DiscoveryNode(nodeName, nodeId, hostName, hostAddress, address, attributes, version);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeName);
        out.writeString(nodeId);
        out.writeString(hostName);
        out.writeString(hostAddress);
        addressToStream(out, address);
        out.writeVInt(attributes.size());
        for (ObjectObjectCursor<String, String> entry : attributes) {
            out.writeString(entry.key);
            out.writeString(entry.value);
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
        for (ObjectObjectCursor<String, String> attr : attributes) {
            builder.field(attr.key, attr.value);
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }

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
