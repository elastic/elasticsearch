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
 * Response when deleting a role mapping. If the mapping is successfully
 * deleted, the response returns a boolean field "found" {@code true}.
 * Otherwise, "found" is set to {@code false}.
 */
public final class DeleteRoleMappingResponse {

    private final boolean found;

    public DeleteRoleMappingResponse(boolean deleted) {
        this.found = deleted;
    }

    public boolean isFound() {
        return found;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DeleteRoleMappingResponse that = (DeleteRoleMappingResponse) o;
        return found == that.found;
    }

    @Override
    public int hashCode() {
        return Objects.hash(found);
    }

    private static final ConstructingObjectParser<DeleteRoleMappingResponse, Void> PARSER = new ConstructingObjectParser<>(
            "delete_role_mapping_response", true, args -> new DeleteRoleMappingResponse((boolean) args[0]));
    static {
        PARSER.declareBoolean(constructorArg(), new ParseField("found"));
    }

    public static DeleteRoleMappingResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
