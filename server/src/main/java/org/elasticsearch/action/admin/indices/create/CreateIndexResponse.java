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

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A response for a create index action.
 */
public class CreateIndexResponse extends AcknowledgedResponse implements ToXContentObject {

    private static final ParseField SHARDS_ACKNOWLEDGED = new ParseField("shards_acknowledged");
    private static final ParseField INDEX = new ParseField("index");

    private static final ConstructingObjectParser<CreateIndexResponse, Void> PARSER = new ConstructingObjectParser<>("create_index",
        true, args -> new CreateIndexResponse((boolean) args[0], (boolean) args[1], (String) args[2]));

    static {
        declareAcknowledgedField(PARSER);
        PARSER.declareField(constructorArg(), (parser, context) -> parser.booleanValue(), SHARDS_ACKNOWLEDGED,
            ObjectParser.ValueType.BOOLEAN);
        PARSER.declareField(constructorArg(), (parser, context) -> parser.text(), INDEX, ObjectParser.ValueType.STRING);
    }

    private boolean shardsAcknowledged;
    private String index;

    protected CreateIndexResponse() {
    }

    protected CreateIndexResponse(boolean acknowledged, boolean shardsAcknowledged, String index) {
        super(acknowledged);
        assert acknowledged || shardsAcknowledged == false; // if its not acknowledged, then shardsAcknowledged should be false too
        this.shardsAcknowledged = shardsAcknowledged;
        this.index = index;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        readAcknowledged(in);
        shardsAcknowledged = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_5_6_0)) {
            index = in.readString();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeAcknowledged(out);
        out.writeBoolean(shardsAcknowledged);
        if (out.getVersion().onOrAfter(Version.V_5_6_0)) {
            out.writeString(index);
        }
    }

    /**
     * Returns true if the requisite number of shards were started before
     * returning from the index creation operation. If {@link #isAcknowledged()}
     * is false, then this also returns false.
     * 
     * @deprecated use {@link #isShardsAcknowledged()}
     */
    @Deprecated
    public boolean isShardsAcked() {
        return shardsAcknowledged;
    }

    /**
     * Returns true if the requisite number of shards were started before
     * returning from the index creation operation. If {@link #isAcknowledged()}
     * is false, then this also returns false.
     */
    public boolean isShardsAcknowledged() {
        return shardsAcknowledged;
    }

    public String index() {
        return index;
    }

    public void addCustomFields(XContentBuilder builder) throws IOException {
        builder.field(SHARDS_ACKNOWLEDGED.getPreferredName(), isShardsAcknowledged());
        builder.field(INDEX.getPreferredName(), index());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        addAcknowledgedField(builder);
        addCustomFields(builder);
        builder.endObject();
        return builder;
    }

    public static CreateIndexResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }
}
