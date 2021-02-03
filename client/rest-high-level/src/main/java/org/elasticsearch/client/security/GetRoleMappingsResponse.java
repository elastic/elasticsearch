/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Get role mappings response
 */
public final class GetRoleMappingsResponse {

    private final List<ExpressionRoleMapping> mappings;

    public GetRoleMappingsResponse(List<ExpressionRoleMapping> mappings) {
        this.mappings = Collections.unmodifiableList(mappings);
    }

    public List<ExpressionRoleMapping> getMappings() {
        return mappings;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final GetRoleMappingsResponse that = (GetRoleMappingsResponse) o;
        return this.mappings.equals(that.mappings);
    }

    @Override
    public int hashCode() {
        return mappings.hashCode();
    }

    public static GetRoleMappingsResponse fromXContent(XContentParser parser) throws IOException {
        final List<ExpressionRoleMapping> roleMappings = new ArrayList<>();

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            roleMappings.add(ExpressionRoleMapping.PARSER.parse(parser, parser.currentName()));
        }

        return new GetRoleMappingsResponse(roleMappings);
    }
}
