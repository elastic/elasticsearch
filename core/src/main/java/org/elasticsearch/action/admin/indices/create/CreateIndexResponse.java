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

    private static final String SHARDS_ACKNOWLEDGED = "shards_acknowledged";
    private static final String INDEX = "index";

    private static final ParseField SHARDS_ACKNOWLEDGED_PARSER = new ParseField(SHARDS_ACKNOWLEDGED);
    private static final ParseField INDEX_PARSER = new ParseField(INDEX);

    private static final ConstructingObjectParser<CreateIndexResponse, Void> PARSER = new ConstructingObjectParser<>("create_index",
        true, args -> new CreateIndexResponse((boolean) args[0], (boolean) args[1], (String) args[2]));

    static {
        declareAcknowledgedField(PARSER);
        PARSER.declareField(constructorArg(), (parser, context) -> parser.booleanValue(), SHARDS_ACKNOWLEDGED_PARSER,
            ObjectParser.ValueType.BOOLEAN);
        PARSER.declareField(constructorArg(), (parser, context) -> parser.text(), INDEX_PARSER, ObjectParser.ValueType.STRING);
    }

    private boolean shardsAcked;
    private String index;

    protected CreateIndexResponse() {
    }

    protected CreateIndexResponse(boolean acknowledged, boolean shardsAcked, String index) {
        super(acknowledged);
        assert acknowledged || shardsAcked == false; // if its not acknowledged, then shards acked should be false too
        this.shardsAcked = shardsAcked;
        this.index = index;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        readAcknowledged(in);
        shardsAcked = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_5_6_0)) {
            index = in.readString();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeAcknowledged(out);
        out.writeBoolean(shardsAcked);
        if (out.getVersion().onOrAfter(Version.V_5_6_0)) {
            out.writeString(index);
        }
    }

    /**
     * Returns true if the requisite number of shards were started before
     * returning from the index creation operation.  If {@link #isAcknowledged()}
     * is false, then this also returns false.
     */
    public boolean isShardsAcked() {
        return shardsAcked;
    }

    public String index() {
        return index;
    }

    public void addCustomFields(XContentBuilder builder) throws IOException {
        builder.field(SHARDS_ACKNOWLEDGED, isShardsAcked());
        builder.field(INDEX, index());
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
