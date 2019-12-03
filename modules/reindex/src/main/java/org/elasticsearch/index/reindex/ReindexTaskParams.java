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
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ReindexTaskParams implements PersistentTaskParams {

    public static final String NAME = ReindexTask.NAME;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<ReindexTaskParams, Void> PARSER
        = new ConstructingObjectParser<>(NAME, a -> new ReindexTaskParams((Boolean) a[0], (List<List<String>>) a[1],
        (Map<String, String>) a[2]));

    private static String STORE_RESULT = "store_result";
    private static String HEADERS = "headers";
    private static String INDEX_GROUPS = "index_groups";

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), new ParseField(STORE_RESULT));
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> p.list(), new ParseField(INDEX_GROUPS));
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.mapStrings(), new ParseField(HEADERS));
    }

    private final boolean storeResult;
    private final List<? extends Collection<String>> indexGroups;
    private final Map<String, String> headers;

    public ReindexTaskParams(boolean storeResult, List<? extends Collection<String>> indexGroups, Map<String, String> headers) {
        this.storeResult = storeResult;
        this.indexGroups = indexGroups;
        this.headers = headers;
    }

    public ReindexTaskParams(StreamInput in) throws IOException {
        storeResult = in.readBoolean();
        indexGroups = in.readList(StreamInput::readStringList);
        headers = in.readMap(StreamInput::readString, StreamInput::readString);
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
        out.writeCollection(indexGroups, StreamOutput::writeStringCollection);
        out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeString);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(STORE_RESULT, storeResult);
        builder.field(INDEX_GROUPS, indexGroups);
        builder.field(HEADERS, headers);
        return builder.endObject();
    }

    public boolean shouldStoreResult() {
        return storeResult;
    }

    public List<? extends Collection<String>> getIndexGroups() {
        return indexGroups;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public static ReindexTaskParams fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
