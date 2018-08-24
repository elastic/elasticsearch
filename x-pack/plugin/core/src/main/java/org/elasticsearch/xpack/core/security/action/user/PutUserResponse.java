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

package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Response when adding a user to the security index. Returns a
 * single boolean field for whether the user was created or updated.
 */
public class PutUserResponse extends ActionResponse implements ToXContentFragment {

    private boolean created;

    public PutUserResponse() {
    }

    public PutUserResponse(boolean created) {
        this.created = created;
    }

    public boolean created() {
        return created;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field("created", created);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(created);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.created = in.readBoolean();
    }

    private static final ConstructingObjectParser<PutUserResponse, Void> PARSER = new ConstructingObjectParser<>("put_user_response",
        true, args -> new PutUserResponse((boolean) args[0]));
    static {
        PARSER.declareBoolean(constructorArg(), new ParseField("created"));
        PARSER.declareObject((a,b) -> {}, (parser, context) -> null, new ParseField("user")); // ignore the user field!
    }

    public static PutUserResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PutUserResponse that = (PutUserResponse) o;
        return created == that.created;
    }

    @Override
    public int hashCode() {
        return Objects.hash(created);
    }
}
