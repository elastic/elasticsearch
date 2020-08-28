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
package org.elasticsearch.client.ml;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Response indicating if the Job(s) closed or not
 */
public class CloseJobResponse implements ToXContentObject {

    private static final ParseField CLOSED = new ParseField("closed");

    public static final ConstructingObjectParser<CloseJobResponse, Void> PARSER =
        new ConstructingObjectParser<>("close_job_response", true, (a) -> new CloseJobResponse((Boolean)a[0]));

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), CLOSED);
    }

    private boolean closed;

    public CloseJobResponse(boolean closed) {
        this.closed = closed;
    }

    public static CloseJobResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * Has the job closed or not
     * @return boolean value indicating the job closed status
     */
    public boolean isClosed() {
        return closed;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        CloseJobResponse that = (CloseJobResponse) other;
        return isClosed() == that.isClosed();
    }

    @Override
    public int hashCode() {
        return Objects.hash(isClosed());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CLOSED.getPreferredName(), closed);
        builder.endObject();
        return builder;
    }
}
