/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.security.user.privileges.ApplicationPrivilege;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Get application privileges response
 */
public final class GetPrivilegesResponse {

    private Set<ApplicationPrivilege> privileges;

    public Set<ApplicationPrivilege> getPrivileges() {
        return privileges;
    }

    public GetPrivilegesResponse(Collection<ApplicationPrivilege> privileges) {
        this.privileges = Set.copyOf(privileges);
    }

    public static GetPrivilegesResponse fromXContent(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        List<ApplicationPrivilege> privileges = new ArrayList<>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != null) {
            if (token == XContentParser.Token.FIELD_NAME) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    privileges.add(ApplicationPrivilege.PARSER.parse(parser, null));
                }
            }
        }
        return new GetPrivilegesResponse(new HashSet<>(privileges));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetPrivilegesResponse that = (GetPrivilegesResponse) o;
        return Objects.equals(privileges, that.privileges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(privileges);
    }
}
