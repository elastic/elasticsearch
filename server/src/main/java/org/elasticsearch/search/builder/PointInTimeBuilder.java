/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.builder;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A search request with a point in time will execute using the reader contexts associated with that point time
 * instead of the latest reader contexts.
 */
public final class PointInTimeBuilder implements Writeable, ToXContentObject {
    private static final ParseField ID_FIELD = new ParseField("id");
    private static final ParseField KEEP_ALIVE_FIELD = new ParseField("keep_alive");
    private static final ObjectParser<XContentParams, Void> PARSER;

    static {
        PARSER = new ObjectParser<>(SearchSourceBuilder.POINT_IN_TIME.getPreferredName(), XContentParams::new);
        PARSER.declareString((params, id) -> params.id = id, ID_FIELD);
        PARSER.declareField((params, keepAlive) -> params.keepAlive = keepAlive,
            (p, c) -> TimeValue.parseTimeValue(p.text(), KEEP_ALIVE_FIELD.getPreferredName()),
            KEEP_ALIVE_FIELD, ObjectParser.ValueType.STRING);
    }

    private static final class XContentParams {
        private String id;
        private TimeValue keepAlive;
    }

    private final String id;
    private TimeValue keepAlive;

    public PointInTimeBuilder(String id) {
        this.id = Objects.requireNonNull(id);
    }

    public PointInTimeBuilder(StreamInput in) throws IOException {
        id = in.readString();
        keepAlive = in.readOptionalTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeOptionalTimeValue(keepAlive);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(SearchSourceBuilder.POINT_IN_TIME.getPreferredName());
        builder.field(ID_FIELD.getPreferredName(), id);
        if (keepAlive != null) {
            builder.field(KEEP_ALIVE_FIELD.getPreferredName(), keepAlive);
        }
        builder.endObject();
        return builder;
    }

    public static PointInTimeBuilder fromXContent(XContentParser parser) throws IOException {
        final XContentParams params = PARSER.parse(parser, null);
        if (params.id == null) {
            throw new IllegalArgumentException("point int time id is not provided");
        }
        return new PointInTimeBuilder(params.id).setKeepAlive(params.keepAlive);
    }

    /**
     * Returns the id of this point in time
     */
    public String getId() {
        return id;
    }

    /**
     * If specified, the search layer will keep this point in time around for at least the given keep-alive.
     * Otherwise, the point in time will be kept around until the original keep alive elapsed.
     */
    public PointInTimeBuilder setKeepAlive(TimeValue keepAlive) {
        this.keepAlive = keepAlive;
        return this;
    }

    @Nullable
    public TimeValue getKeepAlive() {
        return keepAlive;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final PointInTimeBuilder that = (PointInTimeBuilder) o;
        return Objects.equals(id, that.id) && Objects.equals(keepAlive, that.keepAlive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, keepAlive);
    }
}
