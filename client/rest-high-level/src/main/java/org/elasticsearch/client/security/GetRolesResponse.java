/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.core.Tuple;
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
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        final List<Role> roles = new ArrayList<>();
        final Map<String, Map<String, Object>> transientMetadata = new HashMap<>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
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
