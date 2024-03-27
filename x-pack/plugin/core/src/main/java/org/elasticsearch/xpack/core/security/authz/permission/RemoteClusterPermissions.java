/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class RemoteClusterPermissions implements Writeable, ToXContentObject {

    private final List<RemoteClusterGroup> remoteClusterGroups;

    public static final RemoteClusterPermissions NONE = new RemoteClusterPermissions(List.of());

    public RemoteClusterPermissions(StreamInput in) throws IOException {
        this(List.of()); //TODO: fixme
    }
    private RemoteClusterPermissions(List<RemoteClusterGroup> remoteClusterGroups) {
        this.remoteClusterGroups = remoteClusterGroups;
    }

    public String[] privilegeNames(final String remoteClusterAlias) {
        return
            remoteClusterGroups.stream()
                .filter(group -> group.hasPrivileges(remoteClusterAlias))
                .flatMap(groups -> Arrays.stream(groups.clusterPrivileges)).distinct().sorted().toArray(String[]::new);
    }

    public boolean hasPrivileges(final String remoteClusterAlias) {
        return remoteClusterGroups.stream()
            .anyMatch(remoteIndicesGroup -> remoteIndicesGroup.hasPrivileges(remoteClusterAlias));
    }

    public boolean hasPrivileges(){
        return remoteClusterGroups.isEmpty() == false;
    }

    public List<RemoteClusterGroup> groups() {
        return Collections.unmodifiableList(remoteClusterGroups);
    }

    //TODO: remove this in favor of just constructing the builder
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        //TODO: fixme
//        builder.startObject();
//        builder.array(RoleDescriptor.Fields.PRIVILEGES.getPreferredName(), remoteClusterGroup.clusterPrivileges());
//        builder.array(RoleDescriptor.Fields.CLUSTERS.getPreferredName(), remoteClusterGroup.remoteClusterAliases());
//        builder.endObject();
//        return builder;
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

        //TODO: fixme
    }

    public static class Builder {
        final List<RemoteClusterGroup> remoteClusterGroupsList; //aliases -> permissions

        public Builder() {
            this.remoteClusterGroupsList = new ArrayList<>();
        }

        public Builder addGroup(RemoteClusterGroup remoteClusterGroup)

        {
            remoteClusterGroupsList.add(remoteClusterGroup);
            return this;
        }

        public RemoteClusterPermissions build() {
            return new RemoteClusterPermissions(remoteClusterGroupsList);
        }
    }

    //TODO: pull out to top level, implement toXContent, Writable
    // and replace org.elasticsearch.xpack.core.security.authz.RoleDescriptor.RemoteClusterPrivileges
    public record RemoteClusterGroup(
        String[] clusterPrivileges,
        String[] remoteClusterAliases,
        StringMatcher remoteClusterAliasMatcher

    ) {
        public RemoteClusterGroup(String[] clusterPrivileges, String[] remoteClusterAliases) {
            this(clusterPrivileges,remoteClusterAliases, StringMatcher.of(remoteClusterAliases));
        }

        public boolean hasPrivileges(final String remoteClusterAlias) {
            return remoteClusterAliasMatcher.test(remoteClusterAlias);
        }
    }
}
