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

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Get role mappings response
 */
public final class GetRoleMappingsResponse {

    private List<ExpressionRoleMapping> mappings;

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
        List<ExpressionRoleMapping> roleMappings = new ArrayList<>();

        parser.nextToken();
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            // Move to the role mapping field
            parser.nextToken();
            while (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                String name = parser.currentName();
                ExpressionRoleMapping.Builder builder = ExpressionRoleMapping.PARSER.parse(parser, null);
                roleMappings.add(builder.build(name));
                // Move to the next role mapping field
                parser.nextToken();
            }
        } else {
            throw new ParsingException(parser.getTokenLocation(), "Expected token [{}] but found token [{}]",
                    XContentParser.Token.START_OBJECT, parser.currentToken());
        }

        return new GetRoleMappingsResponse(roleMappings);
    }
}
