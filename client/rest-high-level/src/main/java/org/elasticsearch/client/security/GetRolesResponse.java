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

import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Response when requesting one or more roles.
 * Returns a List of {@link Role} objects
 */
public final class GetRolesResponse {

    private final List<Role> roles;
    private final Map<String, Map<String, Object>> transientMetadataMap;

    GetRolesResponse(List<Role> roles, Map<String, Map<String, Object>> transientMetadataMap) {
        this.roles = Collections.unmodifiableList(roles);
        this.transientMetadataMap = Collections.unmodifiableMap(transientMetadataMap);
    }

    public List<Role> getRoles() {
        return roles;
    }

    public Map<String, Map<String, Object>> getTransientMetadataMap() {
        return transientMetadataMap;
    }

    public Map<String, Object> getTransientMetadata(String roleName) {
        return transientMetadataMap.get(roleName);
    }

    public static GetRolesResponse fromXContent(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        final List<Role> roles = new ArrayList<>();
        final Map<String, Map<String, Object>> transientMetadata = new HashMap<>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
            final Tuple<Role, Map<String, Object>> roleAndTransientMetadata = Role.PARSER.parse(parser, parser.currentName());
            roles.add(roleAndTransientMetadata.v1());
            transientMetadata.put(roleAndTransientMetadata.v1().getName(), roleAndTransientMetadata.v2());
        }
        return new GetRolesResponse(roles, transientMetadata);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetRolesResponse response = (GetRolesResponse) o;
        return Objects.equals(roles, response.roles)
                && Objects.equals(transientMetadataMap, response.transientMetadataMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(roles, transientMetadataMap);
    }
}
