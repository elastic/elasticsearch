/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

/**
 * A holder for a Role that contains user-readable information about the Role
 * without containing the actual Role object.
 */
public class DefaultRoleDescriptor implements RoleDescriptor {

    private final String name;
    private final String[] clusterPrivileges;
    private final ConfigurableClusterPrivilege[] configurableClusterPrivileges;
    private final IndicesPrivileges[] indicesPrivileges;
    private final ApplicationResourcePrivileges[] applicationPrivileges;
    private final String[] runAs;
    private final RemoteIndicesPrivileges[] remoteIndicesPrivileges;
    private final Map<String, Object> metadata;
    private final Map<String, Object> transientMetadata;

    public DefaultRoleDescriptor(
        String name,
        @Nullable String[] clusterPrivileges,
        @Nullable IndicesPrivileges[] indicesPrivileges,
        @Nullable String[] runAs
    ) {
        this(name, clusterPrivileges, indicesPrivileges, runAs, null);
    }

    /**
     * @deprecated Use {@link #DefaultRoleDescriptor(String, String[], IndicesPrivileges[], ApplicationResourcePrivileges[],
     * ConfigurableClusterPrivilege[], String[], Map, Map, RemoteIndicesPrivileges[])}
     */
    @Deprecated
    public DefaultRoleDescriptor(
        String name,
        @Nullable String[] clusterPrivileges,
        @Nullable IndicesPrivileges[] indicesPrivileges,
        @Nullable String[] runAs,
        @Nullable Map<String, Object> metadata
    ) {
        this(name, clusterPrivileges, indicesPrivileges, runAs, metadata, null);
    }

    /**
     * @deprecated Use {@link #DefaultRoleDescriptor(String, String[], IndicesPrivileges[], ApplicationResourcePrivileges[],
     * ConfigurableClusterPrivilege[], String[], Map, Map, RemoteIndicesPrivileges[])}
     */
    @Deprecated
    public DefaultRoleDescriptor(
        String name,
        @Nullable String[] clusterPrivileges,
        @Nullable IndicesPrivileges[] indicesPrivileges,
        @Nullable String[] runAs,
        @Nullable Map<String, Object> metadata,
        @Nullable Map<String, Object> transientMetadata
    ) {
        this(name, clusterPrivileges, indicesPrivileges, null, null, runAs, metadata, transientMetadata, RemoteIndicesPrivileges.NONE);
    }

    public DefaultRoleDescriptor(
        String name,
        @Nullable String[] clusterPrivileges,
        @Nullable IndicesPrivileges[] indicesPrivileges,
        @Nullable ApplicationResourcePrivileges[] applicationPrivileges,
        @Nullable ConfigurableClusterPrivilege[] configurableClusterPrivileges,
        @Nullable String[] runAs,
        @Nullable Map<String, Object> metadata,
        @Nullable Map<String, Object> transientMetadata
    ) {
        this(
            name,
            clusterPrivileges,
            indicesPrivileges,
            applicationPrivileges,
            configurableClusterPrivileges,
            runAs,
            metadata,
            transientMetadata,
            RemoteIndicesPrivileges.NONE
        );
    }

    public DefaultRoleDescriptor(
        String name,
        @Nullable String[] clusterPrivileges,
        @Nullable IndicesPrivileges[] indicesPrivileges,
        @Nullable ApplicationResourcePrivileges[] applicationPrivileges,
        @Nullable ConfigurableClusterPrivilege[] configurableClusterPrivileges,
        @Nullable String[] runAs,
        @Nullable Map<String, Object> metadata,
        @Nullable Map<String, Object> transientMetadata,
        @Nullable RemoteIndicesPrivileges[] remoteIndicesPrivileges
    ) {
        this.name = name;
        this.clusterPrivileges = clusterPrivileges != null ? clusterPrivileges : Strings.EMPTY_ARRAY;
        this.configurableClusterPrivileges = sortConfigurableClusterPrivileges(configurableClusterPrivileges);
        this.indicesPrivileges = indicesPrivileges != null ? indicesPrivileges : IndicesPrivileges.NONE;
        this.applicationPrivileges = applicationPrivileges != null ? applicationPrivileges : ApplicationResourcePrivileges.NONE;
        this.runAs = runAs != null ? runAs : Strings.EMPTY_ARRAY;
        this.metadata = metadata != null ? Collections.unmodifiableMap(metadata) : Collections.emptyMap();
        this.transientMetadata = transientMetadata != null
            ? Collections.unmodifiableMap(transientMetadata)
            : Collections.singletonMap("enabled", true);
        this.remoteIndicesPrivileges = remoteIndicesPrivileges != null ? remoteIndicesPrivileges : RemoteIndicesPrivileges.NONE;
    }

    public DefaultRoleDescriptor(StreamInput in) throws IOException {
        this.name = in.readString();
        this.clusterPrivileges = in.readStringArray();
        int size = in.readVInt();
        this.indicesPrivileges = new IndicesPrivileges[size];
        for (int i = 0; i < size; i++) {
            indicesPrivileges[i] = new IndicesPrivileges(in);
        }
        this.runAs = in.readStringArray();
        this.metadata = in.readMap();
        this.transientMetadata = in.readMap();

        this.applicationPrivileges = in.readArray(ApplicationResourcePrivileges::new, ApplicationResourcePrivileges[]::new);
        this.configurableClusterPrivileges = ConfigurableClusterPrivileges.readArray(in);
        if (in.getTransportVersion().onOrAfter(TRANSPORT_VERSION_REMOTE_INDICES)) {
            this.remoteIndicesPrivileges = in.readArray(RemoteIndicesPrivileges::new, RemoteIndicesPrivileges[]::new);
        } else {
            this.remoteIndicesPrivileges = RemoteIndicesPrivileges.NONE;
        }
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String[] getClusterPrivileges() {
        return this.clusterPrivileges;
    }

    @Override
    public ConfigurableClusterPrivilege[] getConditionalClusterPrivileges() {
        return this.configurableClusterPrivileges;
    }

    @Override
    public IndicesPrivileges[] getIndicesPrivileges() {
        return this.indicesPrivileges;
    }

    @Override
    public RemoteIndicesPrivileges[] getRemoteIndicesPrivileges() {
        return this.remoteIndicesPrivileges;
    }

    @Override
    public boolean hasRemoteIndicesPrivileges() {
        return remoteIndicesPrivileges.length != 0;
    }

    @Override
    public ApplicationResourcePrivileges[] getApplicationPrivileges() {
        return this.applicationPrivileges;
    }

    @Override
    public boolean hasClusterPrivileges() {
        return clusterPrivileges.length != 0;
    }

    @Override
    public boolean hasApplicationPrivileges() {
        return applicationPrivileges.length != 0;
    }

    @Override
    public boolean hasConfigurableClusterPrivileges() {
        return configurableClusterPrivileges.length != 0;
    }

    @Override
    public boolean hasRunAs() {
        return runAs.length != 0;
    }

    @Override
    public String[] getRunAs() {
        return this.runAs;
    }

    @Override
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    @Override
    public Map<String, Object> getTransientMetadata() {
        return transientMetadata;
    }

    @Override
    public boolean isUsingDocumentOrFieldLevelSecurity() {
        return Arrays.stream(indicesPrivileges).anyMatch(ip -> ip.isUsingDocumentLevelSecurity() || ip.isUsingFieldLevelSecurity());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Role[");
        sb.append("name=").append(name);
        sb.append(", cluster=[").append(Strings.arrayToCommaDelimitedString(clusterPrivileges));
        sb.append("], global=[").append(Strings.arrayToCommaDelimitedString(configurableClusterPrivileges));
        sb.append("], indicesPrivileges=[");
        for (IndicesPrivileges group : indicesPrivileges) {
            sb.append(group.toString()).append(",");
        }
        sb.append("], applicationPrivileges=[");
        for (ApplicationResourcePrivileges privilege : applicationPrivileges) {
            sb.append(privilege.toString()).append(",");
        }
        sb.append("], runAs=[").append(Strings.arrayToCommaDelimitedString(runAs));
        sb.append("], metadata=[");
        sb.append(metadata);
        sb.append("]");
        sb.append(", remoteIndicesPrivileges=[");
        for (RemoteIndicesPrivileges group : remoteIndicesPrivileges) {
            sb.append(group.toString()).append(",");
        }
        sb.append("]");
        sb.append("]");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DefaultRoleDescriptor that = (DefaultRoleDescriptor) o;

        if (name.equals(that.name) == false) return false;
        if (Arrays.equals(clusterPrivileges, that.clusterPrivileges) == false) return false;
        if (Arrays.equals(configurableClusterPrivileges, that.configurableClusterPrivileges) == false) return false;
        if (Arrays.equals(indicesPrivileges, that.indicesPrivileges) == false) return false;
        if (Arrays.equals(applicationPrivileges, that.applicationPrivileges) == false) return false;
        if (metadata.equals(that.getMetadata()) == false) return false;
        if (Arrays.equals(runAs, that.runAs) == false) return false;
        return Arrays.equals(remoteIndicesPrivileges, that.remoteIndicesPrivileges);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + Arrays.hashCode(clusterPrivileges);
        result = 31 * result + Arrays.hashCode(configurableClusterPrivileges);
        result = 31 * result + Arrays.hashCode(indicesPrivileges);
        result = 31 * result + Arrays.hashCode(applicationPrivileges);
        result = 31 * result + Arrays.hashCode(runAs);
        result = 31 * result + metadata.hashCode();
        result = 31 * result + Arrays.hashCode(remoteIndicesPrivileges);
        return result;
    }

    @Override
    public boolean isEmpty() {
        return clusterPrivileges.length == 0
            && configurableClusterPrivileges.length == 0
            && indicesPrivileges.length == 0
            && applicationPrivileges.length == 0
            && runAs.length == 0
            && metadata.size() == 0
            && remoteIndicesPrivileges.length == 0;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return toXContent(builder, params, false);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params, boolean docCreation) throws IOException {
        builder.startObject();
        builder.array(Fields.CLUSTER.getPreferredName(), clusterPrivileges);
        if (configurableClusterPrivileges.length != 0) {
            builder.field(Fields.GLOBAL.getPreferredName());
            ConfigurableClusterPrivileges.toXContent(builder, params, Arrays.asList(configurableClusterPrivileges));
        }
        builder.xContentList(Fields.INDICES.getPreferredName(), indicesPrivileges);
        builder.xContentList(Fields.APPLICATIONS.getPreferredName(), applicationPrivileges);
        if (runAs != null) {
            builder.array(Fields.RUN_AS.getPreferredName(), runAs);
        }
        builder.field(Fields.METADATA.getPreferredName(), metadata);
        if (docCreation) {
            builder.field(Fields.TYPE.getPreferredName(), ROLE_TYPE);
        } else {
            builder.field(Fields.TRANSIENT_METADATA.getPreferredName(), transientMetadata);
        }
        if (hasRemoteIndicesPrivileges()) {
            builder.xContentList(Fields.REMOTE_INDICES.getPreferredName(), remoteIndicesPrivileges);
        }
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeStringArray(clusterPrivileges);
        out.writeVInt(indicesPrivileges.length);
        for (IndicesPrivileges group : indicesPrivileges) {
            group.writeTo(out);
        }
        out.writeStringArray(runAs);
        out.writeGenericMap(metadata);
        out.writeGenericMap(transientMetadata);
        out.writeArray(ApplicationResourcePrivileges::write, applicationPrivileges);
        ConfigurableClusterPrivileges.writeArray(out, getConditionalClusterPrivileges());
        if (out.getTransportVersion().onOrAfter(TRANSPORT_VERSION_REMOTE_INDICES)) {
            out.writeArray(remoteIndicesPrivileges);
        }
    }

    private static ConfigurableClusterPrivilege[] sortConfigurableClusterPrivileges(
        ConfigurableClusterPrivilege[] configurableClusterPrivileges
    ) {
        if (null == configurableClusterPrivileges) {
            return ConfigurableClusterPrivileges.EMPTY_ARRAY;
        } else if (configurableClusterPrivileges.length < 2) {
            return configurableClusterPrivileges;
        } else {
            ConfigurableClusterPrivilege[] configurableClusterPrivilegesCopy = Arrays.copyOf(
                configurableClusterPrivileges,
                configurableClusterPrivileges.length
            );
            Arrays.sort(configurableClusterPrivilegesCopy, Comparator.comparingInt(o -> o.getCategory().ordinal()));
            return configurableClusterPrivilegesCopy;
        }
    }
}
