/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.io.IOException;

/**
 * Represents a group of permissions for a remote cluster. For example:
 * <code>
 {
    "privileges" : ["monitor_enrich"],
    "clusters" : ["*"]
 }
 * </code>
 */
public class RemoteClusterPermissionGroup implements ToXContentObject {

    private final String[] clusterPrivileges;
    private final String[] remoteClusterAliases;
    private final StringMatcher remoteClusterAliasMatcher;

    private RemoteClusterPermissionGroup(
        String[] clusterPrivileges,
        String[] remoteClusterAliases,
        StringMatcher remoteClusterAliasMatcher
    ) {
        this.clusterPrivileges = clusterPrivileges;
        this.remoteClusterAliases = remoteClusterAliases;
        this.remoteClusterAliasMatcher = remoteClusterAliasMatcher;
    }

    public RemoteClusterPermissionGroup(String[] clusterPrivileges, String[] remoteClusterAliases) {
        this(clusterPrivileges, remoteClusterAliases, StringMatcher.of(remoteClusterAliases));
    }

    public boolean hasPrivileges(final String remoteClusterAlias) {
        return remoteClusterAliasMatcher.test(remoteClusterAlias);
    }

    public String[] clusterPrivileges() {
        return clusterPrivileges;
    }

    public String[] remoteClusterAliases() {
        return remoteClusterAliases;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.array(RoleDescriptor.Fields.PRIVILEGES.getPreferredName(), clusterPrivileges);
        builder.array(RoleDescriptor.Fields.CLUSTERS.getPreferredName(), remoteClusterAliases);
        builder.endObject();
        return builder;
    }
}
