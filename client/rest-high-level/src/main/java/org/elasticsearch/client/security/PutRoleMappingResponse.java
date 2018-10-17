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
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Response when adding/updating a role mapping. Returns a boolean field for
 * whether the role mapping was created or updated.
 */
public final class PutRoleMappingResponse {

    private final boolean created;

    public PutRoleMappingResponse(boolean created) {
        this.created = created;
    }

    public boolean isCreated() {
        return created;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PutRoleMappingResponse that = (PutRoleMappingResponse) o;
        return created == that.created;
    }

    @Override
    public int hashCode() {
        return Objects.hash(created);
    }

    private static final ConstructingObjectParser<PutRoleMappingResponse, Void> PARSER = new ConstructingObjectParser<>(
            "put_role_mapping_response", true, args -> new PutRoleMappingResponse((boolean) args[0]));
    static {
        ConstructingObjectParser<Boolean, Void> roleMappingParser = new ConstructingObjectParser<>(
                "put_role_mapping_response.role_mapping", true, args -> (Boolean) args[0]);
        roleMappingParser.declareBoolean(constructorArg(), new ParseField("created"));
        PARSER.declareObject(constructorArg(), roleMappingParser::parse, new ParseField("role_mapping"));
    }

    public static PutRoleMappingResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
