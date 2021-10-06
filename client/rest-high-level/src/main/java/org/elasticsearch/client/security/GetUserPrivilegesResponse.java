/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.security.user.privileges.ApplicationResourcePrivileges;
import org.elasticsearch.client.security.user.privileges.GlobalPrivileges;
import org.elasticsearch.client.security.user.privileges.UserIndicesPrivileges;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * The response for the {@link org.elasticsearch.client.SecurityClient#getUserPrivileges(RequestOptions)} API.
 * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-get-user-privileges.html">the API docs</a>
 */
public class GetUserPrivilegesResponse {

    private static final ConstructingObjectParser<GetUserPrivilegesResponse, Void> PARSER = new ConstructingObjectParser<>(
        "get_user_privileges_response", true, GetUserPrivilegesResponse::buildResponseFromParserArgs);

    @SuppressWarnings("unchecked")
    private static GetUserPrivilegesResponse buildResponseFromParserArgs(Object[] args) {
        return new GetUserPrivilegesResponse(
            (Collection<String>) args[0],
            (Collection<GlobalPrivileges>) args[1],
            (Collection<UserIndicesPrivileges>) args[2],
            (Collection<ApplicationResourcePrivileges>) args[3],
            (Collection<String>) args[4]
        );
    }

    static {
        PARSER.declareStringArray(constructorArg(), new ParseField("cluster"));
        PARSER.declareObjectArray(constructorArg(), (parser, ignore) -> GlobalPrivileges.fromXContent(parser),
            new ParseField("global"));
        PARSER.declareObjectArray(constructorArg(), (parser, ignore) -> UserIndicesPrivileges.fromXContent(parser),
            new ParseField("indices"));
        PARSER.declareObjectArray(constructorArg(), (parser, ignore) -> ApplicationResourcePrivileges.fromXContent(parser),
            new ParseField("applications"));
        PARSER.declareStringArray(constructorArg(), new ParseField("run_as"));
    }

    public static GetUserPrivilegesResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private Set<String> clusterPrivileges;
    private Set<GlobalPrivileges> globalPrivileges;
    private Set<UserIndicesPrivileges> indicesPrivileges;
    private Set<ApplicationResourcePrivileges> applicationPrivileges;
    private Set<String> runAsPrivilege;

    public GetUserPrivilegesResponse(Collection<String> clusterPrivileges, Collection<GlobalPrivileges> globalPrivileges,
                                     Collection<UserIndicesPrivileges> indicesPrivileges,
                                     Collection<ApplicationResourcePrivileges> applicationPrivileges, Collection<String> runAsPrivilege) {
        this.clusterPrivileges = Collections.unmodifiableSet(new LinkedHashSet<>(clusterPrivileges));
        this.globalPrivileges = Collections.unmodifiableSet(new LinkedHashSet<>(globalPrivileges));
        this.indicesPrivileges = Collections.unmodifiableSet(new LinkedHashSet<>(indicesPrivileges));
        this.applicationPrivileges = Collections.unmodifiableSet(new LinkedHashSet<>(applicationPrivileges));
        this.runAsPrivilege = Collections.unmodifiableSet(new LinkedHashSet<>(runAsPrivilege));
    }

    public Set<String> getClusterPrivileges() {
        return clusterPrivileges;
    }

    public Set<GlobalPrivileges> getGlobalPrivileges() {
        return globalPrivileges;
    }

    public Set<UserIndicesPrivileges> getIndicesPrivileges() {
        return indicesPrivileges;
    }

    public Set<ApplicationResourcePrivileges> getApplicationPrivileges() {
        return applicationPrivileges;
    }

    public Set<String> getRunAsPrivilege() {
        return runAsPrivilege;
    }

    @Override
    public String toString() {
        return "GetUserPrivilegesResponse{" +
            "clusterPrivileges=" + clusterPrivileges +
            ", globalPrivileges=" + globalPrivileges +
            ", indicesPrivileges=" + indicesPrivileges +
            ", applicationPrivileges=" + applicationPrivileges +
            ", runAsPrivilege=" + runAsPrivilege +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final GetUserPrivilegesResponse that = (GetUserPrivilegesResponse) o;
        return Objects.equals(this.clusterPrivileges, that.clusterPrivileges) &&
            Objects.equals(this.globalPrivileges, that.globalPrivileges) &&
            Objects.equals(this.indicesPrivileges, that.indicesPrivileges) &&
            Objects.equals(this.applicationPrivileges, that.applicationPrivileges) &&
            Objects.equals(this.runAsPrivilege, that.runAsPrivilege);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterPrivileges, globalPrivileges, indicesPrivileges, applicationPrivileges, runAsPrivilege);
    }
}
