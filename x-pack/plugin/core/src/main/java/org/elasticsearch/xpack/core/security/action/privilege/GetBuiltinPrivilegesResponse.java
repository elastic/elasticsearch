/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * Response containing built-in (cluster/index) privileges
 */
public final class GetBuiltinPrivilegesResponse extends ActionResponse {

    private final String[] clusterPrivileges;
    private final String[] indexPrivileges;
    private final String[] remoteClusterPrivileges;

    // used by serverless
    public GetBuiltinPrivilegesResponse(Collection<String> clusterPrivileges, Collection<String> indexPrivileges) {
        this(clusterPrivileges, indexPrivileges, Collections.emptySet());
    }

    public GetBuiltinPrivilegesResponse(
        Collection<String> clusterPrivileges,
        Collection<String> indexPrivileges,
        Collection<String> remoteClusterPrivileges
    ) {
        this.clusterPrivileges = Objects.requireNonNull(clusterPrivileges, "Cluster privileges cannot be null")
            .toArray(Strings.EMPTY_ARRAY);
        this.indexPrivileges = Objects.requireNonNull(indexPrivileges, "Index privileges cannot be null").toArray(Strings.EMPTY_ARRAY);
        this.remoteClusterPrivileges = Objects.requireNonNull(remoteClusterPrivileges, "Remote cluster privileges cannot be null")
            .toArray(Strings.EMPTY_ARRAY);
    }

    public GetBuiltinPrivilegesResponse() {
        this(Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
    }

    public String[] getClusterPrivileges() {
        return clusterPrivileges;
    }

    public String[] getIndexPrivileges() {
        return indexPrivileges;
    }

    public String[] getRemoteClusterPrivileges() {
        return remoteClusterPrivileges;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }
}
