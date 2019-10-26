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

package org.elasticsearch.client.security;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

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
        return Objects.equals(this.clusterPrivileges, that.clusterPrivileges)
        && Objects.equals(this.indexPrivileges, that.indexPrivileges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterPrivileges, indexPrivileges);
    }


    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<GetBuiltinPrivilegesResponse, Void> PARSER = new ConstructingObjectParser<>(
        "get_builtin_privileges", true,
        args -> new GetBuiltinPrivilegesResponse((Collection<String>) args[0], (Collection<String>) args[1]));

    static {
        PARSER.declareStringArray(constructorArg(), new ParseField("cluster"));
        PARSER.declareStringArray(constructorArg(), new ParseField("index"));
    }
}
