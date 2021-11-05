/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Get builtin privileges response
 */
public final class GetBuiltinPrivilegesResponse {

    private final Set<String> clusterPrivileges;
    private final Set<String> indexPrivileges;

    public GetBuiltinPrivilegesResponse(Collection<String> cluster, Collection<String> index) {
        this.clusterPrivileges = Set.copyOf(cluster);
        this.indexPrivileges = Set.copyOf(index);
    }

    public Set<String> getClusterPrivileges() {
        return clusterPrivileges;
    }

    public Set<String> getIndexPrivileges() {
        return indexPrivileges;
    }

    public static GetBuiltinPrivilegesResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetBuiltinPrivilegesResponse that = (GetBuiltinPrivilegesResponse) o;
        return Objects.equals(this.clusterPrivileges, that.clusterPrivileges) && Objects.equals(this.indexPrivileges, that.indexPrivileges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterPrivileges, indexPrivileges);
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<GetBuiltinPrivilegesResponse, Void> PARSER = new ConstructingObjectParser<>(
        "get_builtin_privileges",
        true,
        args -> new GetBuiltinPrivilegesResponse((Collection<String>) args[0], (Collection<String>) args[1])
    );

    static {
        PARSER.declareStringArray(constructorArg(), new ParseField("cluster"));
        PARSER.declareStringArray(constructorArg(), new ParseField("index"));
    }
}
