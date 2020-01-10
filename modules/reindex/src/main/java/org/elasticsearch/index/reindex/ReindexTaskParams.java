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

package org.elasticsearch.index.reindex;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.persistent.PersistentTaskParams;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class ReindexTaskParams implements PersistentTaskParams {

    public static final String NAME = ReindexTask.NAME;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<ReindexTaskParams, Void> PARSER
        = new ConstructingObjectParser<>(NAME, a -> new ReindexTaskParams((Boolean) a[0], (Map<String, String>) a[1], (float) a[2]));

    private static String STORE_RESULT = "store_result";
    private static String HEADERS = "headers";
    private static String REQUESTS_PER_SECOND = "requests_per_second";

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), new ParseField(STORE_RESULT));
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.mapStrings(), new ParseField(HEADERS));
        PARSER.declareFloat(ConstructingObjectParser.constructorArg(), new ParseField(REQUESTS_PER_SECOND));
    }

    private final boolean storeResult;
    private final Map<String, String> headers;
    private final float requestsPerSecond;

    public ReindexTaskParams(boolean storeResult, Map<String, String> headers, float requestsPerSecond) {
        this.storeResult = storeResult;
        this.headers = headers;
        this.requestsPerSecond = requestsPerSecond;
    }

    public ReindexTaskParams(StreamInput in) throws IOException {
        storeResult = in.readBoolean();
        headers = in.readMap(StreamInput::readString, StreamInput::readString);
        requestsPerSecond = in.readFloat();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        // TODO: version
        return Version.V_8_0_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(storeResult);
        out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeString);
        out.writeFloat(requestsPerSecond);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(STORE_RESULT, storeResult);
        builder.field(HEADERS, headers);
        builder.field(REQUESTS_PER_SECOND, requestsPerSecond);
        return builder.endObject();
    }

    public boolean shouldStoreResult() {
        return storeResult;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public float getRequestsPerSecond() {
        return requestsPerSecond;
    }

    public static ReindexTaskParams fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReindexTaskParams that = (ReindexTaskParams) o;
        return storeResult == that.storeResult &&
            Float.compare(that.requestsPerSecond, requestsPerSecond) == 0 &&
            Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(storeResult, headers, requestsPerSecond);
    }
}
