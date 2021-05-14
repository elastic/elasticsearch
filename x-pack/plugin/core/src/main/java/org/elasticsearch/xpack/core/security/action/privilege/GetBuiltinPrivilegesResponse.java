/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * Response containing one or more application privileges retrieved from the security index
 */
public final class GetBuiltinPrivilegesResponse extends ActionResponse {

    private String[] clusterPrivileges;
    private String[] indexPrivileges;

    public GetBuiltinPrivilegesResponse(String[] clusterPrivileges, String[] indexPrivileges) {
        this.clusterPrivileges = Objects.requireNonNull(clusterPrivileges, "Cluster privileges cannot be null");
        this.indexPrivileges =  Objects.requireNonNull(indexPrivileges, "Index privileges cannot be null");
    }

    public GetBuiltinPrivilegesResponse(Collection<String> clusterPrivileges,
                                        Collection<String> indexPrivileges) {
        this(clusterPrivileges.toArray(Strings.EMPTY_ARRAY), indexPrivileges.toArray(Strings.EMPTY_ARRAY));
    }

    public GetBuiltinPrivilegesResponse() {
        this(Collections.emptySet(), Collections.emptySet());
    }

    public GetBuiltinPrivilegesResponse(StreamInput in) throws IOException {
        super(in);
        this.clusterPrivileges = in.readStringArray();
        this.indexPrivileges = in.readStringArray();
    }

    public String[] getClusterPrivileges() {
        return clusterPrivileges;
    }

    public String[] getIndexPrivileges() {
        return indexPrivileges;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(clusterPrivileges);
        out.writeStringArray(indexPrivileges);
    }
}
