/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.security.support.ServiceAccountInfo;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Response when requesting one or more service accounts.
 * Returns a List of {@link ServiceAccountInfo} objects
 */
public final class GetServiceAccountsResponse {

    private final List<ServiceAccountInfo> serviceAccountInfos;

    public GetServiceAccountsResponse(List<ServiceAccountInfo> serviceAccountInfos) {
        this.serviceAccountInfos = serviceAccountInfos;
    }

    public List<ServiceAccountInfo> getServiceAccountInfos() {
        return serviceAccountInfos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        GetServiceAccountsResponse that = (GetServiceAccountsResponse) o;
        return serviceAccountInfos.equals(that.serviceAccountInfos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceAccountInfos);
    }

    public static GetServiceAccountsResponse fromXContent(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParser.Token token;
        final List<ServiceAccountInfo> serviceAccountInfos = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            final String principal = parser.currentName();
            final Role role = parseRoleDescriptor(parser);
            serviceAccountInfos.add(new ServiceAccountInfo(principal, role));
        }
        return new GetServiceAccountsResponse(serviceAccountInfos);
    }

    private static Role parseRoleDescriptor(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParserUtils.ensureFieldName(parser, parser.nextToken(), "role_descriptor");
        final Role role = Role.PARSER.parse(parser, parser.currentName()).v1();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return role;
    }
}
