/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.StringLiteralDeduplicator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.node.Node;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.elasticsearch.node.NodeRoleSettings.NODE_ROLES_SETTING;

/**
 * A discovery node represents a node that is part of the cluster.
 */
public class DiscoveryNode implements Writeable, ToXContentFragment {

    /**
     * Name of the setting used to enable stateless.
     */
    public static final String STATELESS_ENABLED_SETTING_NAME = "stateless.enabled";

    /**
     * Check if {@link #STATELESS_ENABLED_SETTING_NAME} is present and set to {@code true}, indicating that the node is
     * part of a stateless deployment. When no settings are provided this method falls back to the value of the stateless feature flag;
     * this is convenient for testing purpose as well as all behaviors that rely on node roles to be enabled/disabled by default when no
     * settings are provided.
     *
     * @param settings the node settings
     * @return true if {@link #STATELESS_ENABLED_SETTING_NAME} is present and set
     */
    public static boolean isStateless(final Settings settings) {
        if (settings.isEmpty() == false) {
            return settings.getAsBoolean(STATELESS_ENABLED_SETTING_NAME, false);
        } else {
            // Fallback on stateless feature flag when no settings are provided
            return DiscoveryNodeRole.hasStatelessFeatureFlag();
        }
    }

    /**
     * Check if the serverless feature flag is present and set to {@code true}, indicating that the node is
     * part of a serverless deployment.
     *
     * @return true if the serverless feature flag is present and set
     */
    public static boolean isServerless() {
        return DiscoveryNodeRole.hasServerlessFeatureFlag();
    }

    static final String COORDINATING_ONLY = "coordinating_only";
    public static final TransportVersion EXTERNAL_ID_VERSION = TransportVersion.V_8_3_0;
    public static final Comparator<DiscoveryNode> DISCOVERY_NODE_COMPARATOR = Comparator.comparing(DiscoveryNode::getName)
        .thenComparing(DiscoveryNode::getId);

    public static boolean hasRole(final Settings settings, final DiscoveryNodeRole role) {
        // this method can be called before the o.e.n.NodeRoleSettings.NODE_ROLES_SETTING is initialized
        if (settings.hasValue("node.roles")) {
            return settings.getAsList("node.roles").contains(role.roleName());
        } else {
            return role.isEnabledByDefault(settings);
        }
    }

    public static boolean isMasterNode(final Settings settings) {
        return hasRole(settings, DiscoveryNodeRole.MASTER_ROLE);
    }

    /**
     * Check if the given settings are indicative of having the top-level data role.
     *
     * Note that if you want to test for whether or not the given settings are indicative of any role that can contain data, you should use
     * {@link #canContainData(Settings)}.
     *
     * @param settings the settings
     * @return true if the given settings are indicative of having the top-level data role, otherwise false
     */
    public static boolean hasDataRole(final Settings settings) {
        return hasRole(settings, DiscoveryNodeRole.DATA_ROLE);
    }

    /**
     * Check if the given settings are indicative of any role that can contain data.
     *
     * Note that if you want to test for exactly the data role, you should use {@link #hasDataRole(Settings)}.
     *
     * @param settings the settings
     * @return true if the given settings are indicative of having any role that can contain data, otherwise false
     */
    public static boolean canContainData(final Settings settings) {
        return getRolesFromSettings(settings).stream().anyMatch(DiscoveryNodeRole::canContainData);
    }

    public static boolean isIngestNode(final Settings settings) {
        return hasRole(settings, DiscoveryNodeRole.INGEST_ROLE);
    }

    public static boolean isRemoteClusterClient(final Settings settings) {
        return hasRole(settings, DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE);
    }

    private static boolean isDedicatedFrozenRoles(Set<DiscoveryNodeRole> roles) {
        return roles.contains(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE)
            && roles.stream()
                .filter(DiscoveryNodeRole::canContainData)
                .anyMatch(r -> r != DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE) == false;
    }

    /**
     * Check if the settings are for a dedicated frozen node, i.e. has frozen role and no other data roles.
     */
    public static boolean isDedicatedFrozenNode(final Settings settings) {
        return isDedicatedFrozenRoles(getRolesFromSettings(settings));
    }

    private static final StringLiteralDeduplicator nodeStringDeduplicator = new StringLiteralDeduplicator();

    public record VersionInformation(Version nodeVersion, IndexVersion minIndexVersion, IndexVersion maxIndexVersion) {
        public VersionInformation {
            Objects.requireNonNull(nodeVersion);
            Objects.requireNonNull(minIndexVersion);
            Objects.requireNonNull(maxIndexVersion);
        }
    }

    private final String nodeName;
    private final String nodeId;
    private final String ephemeralId;
    private final String hostName;
    private final String hostAddress;
    private final TransportAddress address;
    private final Map<String, String> attributes;
    private final VersionInformation version;
    private final SortedSet<DiscoveryNodeRole> roles;
    private final Set<String> roleNames;
    private final String externalId;

    /**
     * Creates a new {@link DiscoveryNode}
     * <p>
     * <b>Note:</b> if the version of the node is unknown {@link Version#minimumCompatibilityVersion()} should be used for the current
     * version. it corresponds to the minimum version this elasticsearch version can communicate with. If a higher version is used
     * the node might not be able to communicate with the remote node. After initial handshakes node versions will be discovered
     * and updated.
     * </p>
     *
     * @param nodeName         the nodes name
     * @param nodeId           the nodes unique persistent id. An ephemeral id will be auto generated.
     * @param address          the nodes transport address
     * @param attributes       node attributes
     * @param roles            node roles
     * @param version          the version of the node
     */
    public DiscoveryNode(
        @Nullable String nodeName,
        String nodeId,
        TransportAddress address,
        Map<String, String> attributes,
        Set<DiscoveryNodeRole> roles,
        @Nullable Version version
    ) {
        this(
            nodeName,
            nodeId,
            UUIDs.randomBase64UUID(),
            address.address().getHostString(),
            address.getAddress(),
            address,
            attributes,
            roles,
            version
        );
    }

    /**
     * Creates a new {@link DiscoveryNode}.
     * <p>
     * <b>Note:</b> if the version of the node is unknown {@link Version#minimumCompatibilityVersion()} should be used for the current
     * version. it corresponds to the minimum version this elasticsearch version can communicate with. If a higher version is used
     * the node might not be able to communicate with the remote node. After initial handshakes node versions will be discovered
     * and updated.
     * </p>
     *
     * @param nodeName         the nodes name
     * @param nodeId           the nodes unique persistent id
     * @param ephemeralId      the nodes unique ephemeral id
     * @param hostAddress      the nodes host address
     * @param address          the nodes transport address
     * @param attributes       node attributes
     * @param roles            node roles
     * @param version          the version of the node
     */
    public DiscoveryNode(
        @Nullable String nodeName,
        String nodeId,
        String ephemeralId,
        String hostName,
        String hostAddress,
        TransportAddress address,
        Map<String, String> attributes,
        Set<DiscoveryNodeRole> roles,
        @Nullable Version version
    ) {
        this(nodeName, nodeId, ephemeralId, hostName, hostAddress, address, attributes, roles, expandNodeVersion(version), null);
    }

    static VersionInformation expandNodeVersion(Version version) {
        if (version == null) return null;
        if (version.after(Version.V_8_8_0)) throw new IllegalArgumentException(
            "IndexVersion can only be calculated from Version for <=8.8.0"
        );
        return new VersionInformation(
            version,
            IndexVersion.fromId(version.minimumIndexCompatibilityVersion().id),
            IndexVersion.fromId(version.id)
        );
    }

    /**
     * Creates a new {@link DiscoveryNode}.
     * <p>
     * <b>Note:</b> if the version of the node is unknown {@link Version#minimumCompatibilityVersion()} should be used for the current
     * version. it corresponds to the minimum version this elasticsearch version can communicate with. If a higher version is used
     * the node might not be able to communicate with the remote node. After initial handshakes node versions will be discovered
     * and updated.
     * </p>
     *
     * @param nodeName         the nodes name
     * @param nodeId           the nodes unique persistent id
     * @param ephemeralId      the nodes unique ephemeral id
     * @param hostAddress      the nodes host address
     * @param address          the nodes transport address
     * @param attributes       node attributes
     * @param roles            node roles
     * @param version          node version information
     * @param externalId       the external id used to identify this node by external systems
     */
    public DiscoveryNode(
        @Nullable String nodeName,
        String nodeId,
        String ephemeralId,
        String hostName,
        String hostAddress,
        TransportAddress address,
        Map<String, String> attributes,
        Set<DiscoveryNodeRole> roles,
        @Nullable VersionInformation version,
        @Nullable String externalId
    ) {
        if (nodeName != null) {
            this.nodeName = nodeStringDeduplicator.deduplicate(nodeName);
        } else {
            this.nodeName = "";
        }
        this.nodeId = nodeStringDeduplicator.deduplicate(nodeId);
        this.ephemeralId = nodeStringDeduplicator.deduplicate(ephemeralId);
        this.hostName = nodeStringDeduplicator.deduplicate(hostName);
        assert Strings.hasText(hostAddress);
        this.hostAddress = nodeStringDeduplicator.deduplicate(hostAddress);
        this.address = address;
        this.version = Objects.requireNonNullElse(
            version,
            new VersionInformation(Version.CURRENT, IndexVersion.MINIMUM_COMPATIBLE, IndexVersion.CURRENT)
        );
        this.attributes = Map.copyOf(attributes);
        assert DiscoveryNodeRole.roleNames().stream().noneMatch(attributes::containsKey)
            : "Node roles must not be provided as attributes but saw attributes " + attributes;
        final TreeSet<DiscoveryNodeRole> sortedRoles = new TreeSet<>();
        final String[] roleNames = new String[roles.size()];
        int i = 0;
        for (DiscoveryNodeRole role : roles) {
            sortedRoles.add(role);
            roleNames[i++] = role.roleName();
        }
        this.roles = Collections.unmodifiableSortedSet(sortedRoles);
        this.roleNames = Set.of(roleNames);
        this.externalId = Objects.requireNonNullElse(externalId, this.nodeName);
    }

    /** Creates a DiscoveryNode representing the local node. */
    public static DiscoveryNode createLocal(Settings settings, TransportAddress publishAddress, String nodeId) {
        Map<String, String> attributes = Node.NODE_ATTRIBUTES.getAsMap(settings);
        Set<DiscoveryNodeRole> roles = getRolesFromSettings(settings);
        return new DiscoveryNode(
            Node.NODE_NAME_SETTING.get(settings),
            nodeId,
            UUIDs.randomBase64UUID(),
            publishAddress.address().getHostString(),
            publishAddress.getAddress(),
            publishAddress,
            attributes,
            roles,
            null,
            Node.NODE_EXTERNAL_ID_SETTING.get(settings)
        );
    }

    /** extract node roles from the given settings */
    public static Set<DiscoveryNodeRole> getRolesFromSettings(final Settings settings) {
        return Set.copyOf(NODE_ROLES_SETTING.get(settings));
    }

    private static VersionInformation inferVersionInformation(Version version) {
        if (version.onOrBefore(Version.V_8_8_0)) {
            return new VersionInformation(
                version,
                IndexVersion.fromId(version.minimumIndexCompatibilityVersion().id),
                IndexVersion.fromId(version.id)
            );
        } else {
            return new VersionInformation(version, IndexVersion.MINIMUM_COMPATIBLE, IndexVersion.CURRENT);
        }
    }

    private static final Writeable.Reader<String> readStringLiteral = s -> nodeStringDeduplicator.deduplicate(s.readString());

    /**
     * Creates a new {@link DiscoveryNode} by reading from the stream provided as argument
     * @param in the stream
     * @throws IOException if there is an error while reading from the stream
     */
    public DiscoveryNode(StreamInput in) throws IOException {
        this.nodeName = readStringLiteral.read(in);
        this.nodeId = readStringLiteral.read(in);
        this.ephemeralId = readStringLiteral.read(in);
        this.hostName = readStringLiteral.read(in);
        this.hostAddress = readStringLiteral.read(in);
        this.address = new TransportAddress(in);
        this.attributes = in.readImmutableMap(readStringLiteral, readStringLiteral);
        int rolesSize = in.readVInt();
        final SortedSet<DiscoveryNodeRole> roles = new TreeSet<>();
        final String[] roleNames = new String[rolesSize];
        for (int i = 0; i < rolesSize; i++) {
            final String roleName = in.readString();
            final String roleNameAbbreviation = in.readString();
            final boolean canContainData = in.readBoolean();
            final Optional<DiscoveryNodeRole> maybeRole = DiscoveryNodeRole.maybeGetRoleFromRoleName(roleName);
            if (maybeRole.isEmpty()) {
                roles.add(new DiscoveryNodeRole.UnknownRole(roleName, roleNameAbbreviation, canContainData));
                roleNames[i] = roleName;
            } else {
                final DiscoveryNodeRole role = maybeRole.get();
                assert roleName.equals(role.roleName()) : "role name [" + roleName + "] does not match role [" + role.roleName() + "]";
                assert roleNameAbbreviation.equals(role.roleNameAbbreviation())
                    : "role name abbreviation [" + roleName + "] does not match role [" + role.roleNameAbbreviation() + "]";
                roles.add(role);
                roleNames[i] = role.roleName();
            }
        }
        this.roles = Collections.unmodifiableSortedSet(roles);
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            version = new VersionInformation(Version.readVersion(in), IndexVersion.readVersion(in), IndexVersion.readVersion(in));
        } else {
            version = inferVersionInformation(Version.readVersion(in));
        }
        if (in.getTransportVersion().onOrAfter(EXTERNAL_ID_VERSION)) {
            this.externalId = readStringLiteral.read(in);
        } else {
            this.externalId = nodeName;
        }
        this.roleNames = Set.of(roleNames);
    }

    /**
     * Check if node has the role with the given {@code roleName}.
     *
     * @param roleName role name to check
     * @return true if node has the role of the given name
     */
    public boolean hasRole(String roleName) {
        return this.roleNames.contains(roleName);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeName);
        out.writeString(nodeId);
        out.writeString(ephemeralId);
        out.writeString(hostName);
        out.writeString(hostAddress);
        address.writeTo(out);
        out.writeMap(attributes, StreamOutput::writeString, StreamOutput::writeString);
        out.writeCollection(roles, (o, role) -> {
            o.writeString(role.roleName());
            o.writeString(role.roleNameAbbreviation());
            o.writeBoolean(role.canContainData());
        });
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            Version.writeVersion(version.nodeVersion, out);
            IndexVersion.writeVersion(version.minIndexVersion, out);
            IndexVersion.writeVersion(version.maxIndexVersion, out);
        } else {
            Version.writeVersion(version.nodeVersion(), out);
        }
        if (out.getTransportVersion().onOrAfter(EXTERNAL_ID_VERSION)) {
            out.writeString(externalId);
        }
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
     * The unique ephemeral id of the node. Ephemeral ids are meant to be attached the life span
     * of a node process. When ever a node is restarted, it's ephemeral id is required to change (while it's {@link #getId()}
     * will be read from the data folder and will remain the same across restarts). Since all node attributes and addresses
     * are maintained during the life span of a node process, we can (and are) using the ephemeralId in
     * {@link DiscoveryNode#equals(Object)}.
     */
    public String getEphemeralId() {
        return ephemeralId;
    }

    /**
     * The name of the node.
     */
    public String getName() {
        return this.nodeName;
    }

    /**
     * The external id used to identify this node by external systems
     */
    public String getExternalId() {
        return externalId;
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
    public boolean canContainData() {
        return roles.stream().anyMatch(DiscoveryNodeRole::canContainData);
    }

    /**
     * Can this node become master or not.
     */
    public boolean isMasterNode() {
        return roles.contains(DiscoveryNodeRole.MASTER_ROLE);
    }

    /**
     * Returns a boolean that tells whether this an ingest node or not
     */
    public boolean isIngestNode() {
        return roles.contains(DiscoveryNodeRole.INGEST_ROLE);
    }

    /**
     * Returns whether or not the node can be a remote cluster client.
     *
     * @return true if the node can be a remote cluster client, false otherwise
     */
    public boolean isRemoteClusterClient() {
        return roles.contains(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE);
    }

    /**
     * Returns whether or not the node is a frozen only node, i.e., has data frozen role and no other data roles.
     * @return
     */
    public boolean isDedicatedFrozenNode() {
        return isDedicatedFrozenRoles(getRoles());
    }

    /**
     * Returns a set of all the roles that the node has. The roles are returned in sorted order by the role name.
     * <p>
     * If a node does not have any specific role, the returned set is empty, which means that the node is a coordinating-only node.
     *
     * @return the sorted set of roles
     */
    public Set<DiscoveryNodeRole> getRoles() {
        return roles;
    }

    public VersionInformation getVersionInformation() {
        return this.version;
    }

    public Version getVersion() {
        return this.version.nodeVersion();
    }

    public IndexVersion getMinIndexVersion() {
        return version.minIndexVersion;
    }

    public IndexVersion getMaxIndexVersion() {
        return version.maxIndexVersion;
    }

    public String getHostName() {
        return this.hostName;
    }

    public String getHostAddress() {
        return this.hostAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DiscoveryNode that = (DiscoveryNode) o;

        return ephemeralId.equals(that.ephemeralId);
    }

    @Override
    public int hashCode() {
        // we only need to hash the id because it's highly unlikely that two nodes
        // in our system will have the same id but be different
        // This is done so that this class can be used efficiently as a key in a map
        return ephemeralId.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        appendDescriptionWithoutAttributes(sb);
        if (attributes.isEmpty() == false) {
            sb.append(attributes);
        }
        return sb.toString();
    }

    public void appendDescriptionWithoutAttributes(StringBuilder stringBuilder) {
        if (nodeName.length() > 0) {
            stringBuilder.append('{').append(nodeName).append('}');
        }
        stringBuilder.append('{').append(nodeId).append('}');
        stringBuilder.append('{').append(ephemeralId).append('}');
        if (externalId.length() > 0) {
            stringBuilder.append('{').append(externalId).append('}');
        }
        stringBuilder.append('{').append(hostName).append('}');
        stringBuilder.append('{').append(address).append('}');
        if (roles.isEmpty() == false) {
            stringBuilder.append('{');
            roles.stream().map(DiscoveryNodeRole::roleNameAbbreviation).sorted().forEach(stringBuilder::append);
            stringBuilder.append('}');
        }
        stringBuilder.append('{').append(version.nodeVersion).append('}');
        stringBuilder.append('{').append(version.minIndexVersion).append('-').append(version.maxIndexVersion).append('}');
    }

    public String descriptionWithoutAttributes() {
        final StringBuilder stringBuilder = new StringBuilder();
        appendDescriptionWithoutAttributes(stringBuilder);
        return stringBuilder.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getId());
        builder.field("name", getName());
        builder.field("ephemeral_id", getEphemeralId());
        builder.field("transport_address", getAddress().toString());
        builder.field("external_id", getExternalId());
        builder.stringStringMap("attributes", attributes);
        builder.startArray("roles");
        for (DiscoveryNodeRole role : roles) {
            builder.value(role.roleName());
        }
        builder.endArray();
        builder.field("version", version.nodeVersion);
        builder.field("minIndexVersion", version.minIndexVersion);
        builder.field("maxIndexVersion", version.maxIndexVersion);
        builder.endObject();
        return builder;
    }

    public DiscoveryNode withTransportAddress(TransportAddress transportAddress) {
        return new DiscoveryNode(
            getName(),
            getId(),
            getEphemeralId(),
            getHostName(),
            getHostAddress(),
            transportAddress,
            getAttributes(),
            getRoles(),
            getVersionInformation(),
            getExternalId()
        );
    }

    /**
     * Deduplicate the given string that must be a node id or node name.
     * This method accepts {@code null} input for which it returns {@code null} for convenience when used in deserialization code.
     *
     * @param nodeIdentifier node name or node id or {@code null}
     * @return deduplicated string or {@code null} on null input
     */
    @Nullable
    public static String deduplicateNodeIdentifier(@Nullable String nodeIdentifier) {
        if (nodeIdentifier == null) {
            return null;
        }
        return nodeStringDeduplicator.deduplicate(nodeIdentifier);
    }
}
