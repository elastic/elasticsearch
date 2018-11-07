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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Response for a role being deleted from the native realm
 */
public final class DeleteUserResponse implements ToXContent {

    private final boolean found;
    private static final String PARSE_FIELD = "found";

    public DeleteUserResponse(boolean found) {
        this.found = found;
    }

    public boolean isFound() {
        return this.found;
    }

    private static final ConstructingObjectParser<DeleteUserResponse, Void> PARSER =
        new ConstructingObjectParser<>("delete_user_response", true, args -> new DeleteUserResponse((boolean) args[0]));

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField(PARSE_FIELD));
    }

    public static DeleteUserResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field(PARSE_FIELD, isFound());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DeleteUserResponse that = (DeleteUserResponse) o;
        return isFound() == that.isFound();
    }

    @Override
    public int hashCode() {
        return Objects.hash(found);
    }
}
