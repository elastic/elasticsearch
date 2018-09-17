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
package org.elasticsearch.client.rollup;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class PutRollupJobResponse implements ToXContentObject {

    private final boolean acknowledged;

    public PutRollupJobResponse(final boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    public boolean isAcknowledged() {
        return acknowledged;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PutRollupJobResponse that = (PutRollupJobResponse) o;
        return isAcknowledged() == that.isAcknowledged();
    }

    @Override
    public int hashCode() {
        return Objects.hash(acknowledged);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("acknowledged", isAcknowledged());
        }
        builder.endObject();
        return builder;
    }

    private static final ConstructingObjectParser<PutRollupJobResponse, Void> PARSER
        = new ConstructingObjectParser<>("put_rollup_job_response", true, args -> new PutRollupJobResponse((boolean) args[0]));
    static {
        PARSER.declareBoolean(constructorArg(), new ParseField("acknowledged"));
    }

    public static PutRollupJobResponse fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
