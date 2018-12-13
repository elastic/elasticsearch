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
import org.elasticsearch.common.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureFieldName;

/**
 * Response when adding a role to the native roles store. Returns a
 * single boolean field for whether the role was created (true) or updated (false).
 */
public final class PutRoleResponse {

    private final boolean created;

    public PutRoleResponse(boolean created) {
        this.created = created;
    }

    public boolean isCreated() {
        return created;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PutRoleResponse that = (PutRoleResponse) o;
        return created == that.created;
    }

    @Override
    public int hashCode() {
        return Objects.hash(created);
    }

    private static final ConstructingObjectParser<PutRoleResponse, Void> PARSER = new ConstructingObjectParser<>("put_role_response",
        true, args -> new PutRoleResponse((boolean) args[0]));

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField("created"));
    }

    public static PutRoleResponse fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        // parse extraneous wrapper
        ensureExpectedToken(Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        ensureFieldName(parser, parser.nextToken(), "role");
        parser.nextToken();
        final PutRoleResponse roleResponse = PARSER.parse(parser, null);
        ensureExpectedToken(Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
        return roleResponse;
    }
}
