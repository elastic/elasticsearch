/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.node.NodeRoleSettings.NODE_ROLES_SETTING;

/**
 * A discovery node represents a node that is part of the cluster.
 */
public class DiscoveryNode implements Writeable, ToXContentFragment {

    static final String COORDINATING_ONLY = "coordinating_only";

    public static boolean nodeRequiresLocalStorage(Settings settings) {
        boolean localStorageEnable = Node.NODE_LOCAL_STORAGE_SETTING.get(settings);
        if (localStorageEnable == false && (canContainData(settings) || isMasterNode(settings))) {
            // TODO: make this a proper setting validation logic, requiring multi-settings validation
            throw new IllegalArgumentException("storage can not be disabled for master and data nodes");
        }
        return localStorageEnable;
    }

    public static boolean hasRole(final Settings settings, final DiscoveryNodeRole role) {
        /*
         * This method can be called before the o.e.n.NodeRoleSettings.NODE_ROLES_SETTING is initialized. We do not want to trigger
         * initialization prematurely because that will bake the default roles before plugins have had a chance to register them. Therefore,
         * to avoid initializing this setting prematurely, we avoid using the actual node roles setting instance here.
         */
        if (settings.hasValue("node.roles")) {
            return settings.getAsList("node.roles").contains(role.roleName());
        } else if (role.legacySetting() != null && settings.hasValue(role.legacySetting().getKey())) {
            return role.legacySetting().get(settings);
        } else {
            return role.isEnabledByDefault(settings);
        }
    }

    public static boolean isMasterNode(Settings settings) {
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

    private final String nodeName;
    private final String nodeId;
    private final String ephemeralId;
    private final String hostName;
    private final String hostAddress;
    private final TransportAddress address;
    private final Map<String, String> attributes;
    private final Version version;
    private final SortedSet<DiscoveryNodeRole> roles;

    /**
     * Creates a new {@link DiscoveryNode}
     * <p>
     * <b>Note:</b> if the version of the node is unknown {@link Version#minimumCompatibilityVersion()} should be used for the current
     * version. it corresponds to the minimum version this elasticsearch version can communicate with. If a higher version is used
     * the node might not be able to communicate with the remote node. After initial handshakes node versions will be discovered
     * and updated.
     * </p>
     *
     * @param id               the nodes unique (persistent) node id. This constructor will auto generate a random ephemeral id.
     * @param address          the nodes transport address
     * @param version          the version of the node
     */
    public DiscoveryNode(final String id, TransportAddress address, Version version) {
        this(id, address, Collections.emptyMap(), DiscoveryNodeRole.BUILT_IN_ROLES, version);
    }

    /**
     * Creates a new {@link DiscoveryNode}
     * <p>
     * <b>Note:</b> if the version of the node is unknown {@link Version#minimumCompatibilityVersion()} should be used for the current
     * version. it corresponds to the minimum version this elasticsearch version can communicate with. If a higher version is used
     * the node might not be able to communicate with the remote node. After initial handshakes node versions will be discovered
     * and updated.
     * </p>
     *
     * @param id               the nodes unique (persistent) node id. This constructor will auto generate a random ephemeral id.
     * @param address          the nodes transport address
     * @param attributes       node attributes
     * @param roles            node roles
     * @param version          the version of the node
     */
    public DiscoveryNode(
        String id,
        TransportAddress address,
        Map<String, String> attributes,
        Set<DiscoveryNodeRole> roles,
        Version version
    ) {
        this("", id, address, attributes, roles, version);
    }

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
        String nodeName,
        String nodeId,
        TransportAddress address,
        Map<String, String> attributes,
        Set<DiscoveryNodeRole> roles,
        Version version
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
        String nodeName,
        String nodeId,
        String ephemeralId,
        String hostName,
        String hostAddress,
        TransportAddress address,
        Map<String, String> attributes,
        Set<DiscoveryNodeRole> roles,
        Version version
    ) {
        if (nodeName != null) {
            this.nodeName = nodeName.intern();
        } else {
            this.nodeName = "";
        }
        this.nodeId = nodeId.intern();
        this.ephemeralId = ephemeralId.intern();
        this.hostName = hostName.intern();
        this.hostAddress = hostAddress.intern();
        this.address = address;
        if (version == null) {
            this.version = Version.CURRENT;
        } else {
            this.version = version;
        }
        this.attributes = Collections.unmodifiableMap(attributes);
        assert DiscoveryNode.roleMap.values().stream().noneMatch(role -> attributes.containsKey(role.roleName()))
            : "Node roles must not be provided as attributes but saw attributes " + attributes;
        this.roles = Collections.unmodifiableSortedSet(new TreeSet<>(roles));
    }

    /** Creates a DiscoveryNode representing the local node. */
    public static DiscoveryNode createLocal(Settings settings, TransportAddress publishAddress, String nodeId) {
        Map<String, String> attributes = Node.NODE_ATTRIBUTES.getAsMap(settings);
        Set<DiscoveryNodeRole> roles = getRolesFromSettings(settings);
        return new DiscoveryNode(Node.NODE_NAME_SETTING.get(settings), nodeId, publishAddress, attributes, roles, Version.CURRENT);
    }

    /** extract node roles from the given settings */
    public static Set<DiscoveryNodeRole> getRolesFromSettings(final Settings settings) {
        // are any legacy settings in use?
        boolean usesLegacySettings = getPossibleRoles().stream()
            .anyMatch(s -> s.legacySetting() != null && s.legacySetting().exists(settings));
        if (NODE_ROLES_SETTING.exists(settings) || usesLegacySettings == false) {
            validateLegacySettings(settings, roleMap);
            return Collections.unmodifiableSet(new HashSet<>(NODE_ROLES_SETTING.get(settings)));
        } else {
            return roleMap.values()
                .stream()
                .filter(s -> s.legacySetting() != null && s.legacySetting().get(settings))
                .collect(Collectors.toSet());
        }
    }

    private static void validateLegacySettings(final Settings settings, final Map<String, DiscoveryNodeRole> roleMap) {
        for (final DiscoveryNodeRole role : roleMap.values()) {
            if (role.legacySetting() != null && role.legacySetting().exists(settings)) {
                final String message = String.format(
                    Locale.ROOT,
                    "can not explicitly configure node roles and use legacy role setting [%s]=[%s]",
                    role.legacySetting().getKey(),
                    role.legacySetting().get(settings)
                );
                throw new IllegalArgumentException(message);
            }
        }
    }

    /**
     * Creates a new {@link DiscoveryNode} by reading from the stream provided as argument
     * @param in the stream
     * @throws IOException if there is an error while reading from the stream
     */
    public DiscoveryNode(StreamInput in) throws IOException {
        this.nodeName = in.readString().intern();
        this.nodeId = in.readString().intern();
        this.ephemeralId = in.readString().intern();
        this.hostName = in.readString().intern();
        this.hostAddress = in.readString().intern();
        this.address = new TransportAddress(in);
        int size = in.readVInt();
        this.attributes = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            this.attributes.put(in.readString(), in.readString());
        }
        int rolesSize = in.readVInt();
        final SortedSet<DiscoveryNodeRole> roles = new TreeSet<>();
        if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
            for (int i = 0; i < rolesSize; i++) {
                final String roleName = in.readString();
                final String roleNameAbbreviation = in.readString();
                final boolean canContainData;
                if (in.getVersion().onOrAfter(Version.V_7_10_0)) {
                    canContainData = in.readBoolean();
                } else {
                    canContainData = roleName.equals(DiscoveryNodeRole.DATA_ROLE.roleName());
                }
                final DiscoveryNodeRole role = roleMap.get(roleName);
                if (role == null) {
                    roles.add(new DiscoveryNodeRole.UnknownRole(roleName, roleNameAbbreviation, canContainData));
                } else {
                    assert roleName.equals(role.roleName()) : "role name [" + roleName + "] does not match role [" + role.roleName() + "]";
                    assert roleNameAbbreviation.equals(role.roleNameAbbreviation())
                        : "role name abbreviation [" + roleName + "] does not match role [" + role.roleNameAbbreviation() + "]";
                    roles.add(role);
                }
            }
        } else {
            // an old node will only send us legacy roles since pluggable roles is a new concept
            for (int i = 0; i < rolesSize; i++) {
                final LegacyRole legacyRole = in.readEnum(LegacyRole.class);
                switch (legacyRole) {
                    case MASTER:
                        roles.add(DiscoveryNodeRole.MASTER_ROLE);
                        break;
                    case DATA:
                        roles.add(DiscoveryNodeRole.DATA_ROLE);
                        break;
                    case INGEST:
                        roles.add(DiscoveryNodeRole.INGEST_ROLE);
                        break;
                    default:
                        throw new AssertionError(legacyRole.roleName());
                }
            }
        }
        this.roles = Collections.unmodifiableSortedSet(roles);
        this.version = Version.readVersion(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeName);
        out.writeString(nodeId);
        out.writeString(ephemeralId);
        out.writeString(hostName);
        out.writeString(hostAddress);
        address.writeTo(out);
        out.writeVInt(attributes.size());
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
        if (out.getVersion().onOrAfter(Version.V_7_3_0)) {
            out.writeVInt(roles.size());
            for (final DiscoveryNodeRole role : roles) {
                final DiscoveryNodeRole compatibleRole = role.getCompatibilityRole(out.getVersion());
                out.writeString(compatibleRole.roleName());
                out.writeString(compatibleRole.roleNameAbbreviation());
                if (out.getVersion().onOrAfter(Version.V_7_10_0)) {
                    out.writeBoolean(compatibleRole.canContainData());
                }
            }
        } else {
            // an old node will only understand legacy roles since pluggable roles is a new concept
            final Set<DiscoveryNodeRole> rolesToWrite = roles.stream()
                .map(role -> role.getCompatibilityRole(out.getVersion()))
                .filter(DiscoveryNodeRole.LEGACY_ROLES::contains)
                .collect(Collectors.toSet());

            out.writeVInt(rolesToWrite.size());
            for (final DiscoveryNodeRole role : rolesToWrite) {
                if (role == DiscoveryNodeRole.MASTER_ROLE) {
                    out.writeEnum(LegacyRole.MASTER);
                } else if (role == DiscoveryNodeRole.DATA_ROLE) {
                    out.writeEnum(LegacyRole.DATA);
                } else if (role == DiscoveryNodeRole.INGEST_ROLE) {
                    out.writeEnum(LegacyRole.INGEST);
                }
            }
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
        stringBuilder.append('{').append(hostName).append('}');
        stringBuilder.append('{').append(address).append('}');
        if (roles.isEmpty() == false) {
            stringBuilder.append('{');
            roles.stream().map(DiscoveryNodeRole::roleNameAbbreviation).sorted().forEach(stringBuilder::append);
            stringBuilder.append('}');
        }
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

        builder.startObject("attributes");
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();

        builder.startArray("roles");
        for (DiscoveryNodeRole role : roles) {
            builder.value(role.roleName());
        }
        builder.endArray();

        builder.endObject();
        return builder;
    }

    private static Map<String, DiscoveryNodeRole> rolesToMap(final Stream<DiscoveryNodeRole> roles) {
        return Collections.unmodifiableMap(roles.collect(Collectors.toMap(DiscoveryNodeRole::roleName, Function.identity())));
    }

    private static Map<String, DiscoveryNodeRole> roleMap = rolesToMap(DiscoveryNodeRole.BUILT_IN_ROLES.stream());

    public static DiscoveryNodeRole getRoleFromRoleName(final String roleName) {
        if (roleMap.containsKey(roleName) == false) {
            throw new IllegalArgumentException("unknown role [" + roleName + "]");
        }
        return roleMap.get(roleName);
    }

    public static Collection<DiscoveryNodeRole> getPossibleRoles() {
        return roleMap.values();
    }

    public static void setAdditionalRoles(final Set<DiscoveryNodeRole> additionalRoles) {
        assert additionalRoles.stream().allMatch(r -> r.legacySetting() == null || r.legacySetting().isDeprecated()) : additionalRoles;
        final Map<String, DiscoveryNodeRole> roleNameToPossibleRoles = rolesToMap(
            Stream.concat(DiscoveryNodeRole.BUILT_IN_ROLES.stream(), additionalRoles.stream())
        );
        // collect the abbreviation names into a map to ensure that there are not any duplicate abbreviations
        final Map<String, DiscoveryNodeRole> roleNameAbbreviationToPossibleRoles = Collections.unmodifiableMap(
            roleNameToPossibleRoles.values()
                .stream()
                .collect(Collectors.toMap(DiscoveryNodeRole::roleNameAbbreviation, Function.identity()))
        );
        assert roleNameToPossibleRoles.size() == roleNameAbbreviationToPossibleRoles.size()
            : "roles by name [" + roleNameToPossibleRoles + "], roles by name abbreviation [" + roleNameAbbreviationToPossibleRoles + "]";
        roleMap = roleNameToPossibleRoles;
    }

    public static Set<String> getPossibleRoleNames() {
        return roleMap.keySet();
    }

    /**
     * Enum that holds all the possible roles that that a node can fulfill in a cluster.
     * Each role has its name and a corresponding abbreviation used by cat apis.
     */
    private enum LegacyRole {
        MASTER("master"),
        DATA("data"),
        INGEST("ingest");

        private final String roleName;

        LegacyRole(final String roleName) {
            this.roleName = roleName;
        }

        public String roleName() {
            return roleName;
        }

    }

}
