/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

public class ClearScrollResponse extends ActionResponse implements StatusToXContentObject {

    private static final ParseField SUCCEEDED = new ParseField("succeeded");
    private static final ParseField NUMFREED = new ParseField("num_freed");

    private static final ConstructingObjectParser<ClosePointInTimeResponse, Void> PARSER = new ConstructingObjectParser<>("clear_scroll",
        true, a -> new ClosePointInTimeResponse((boolean) a[0], (int) a[1]));
    static {
        PARSER.declareField(constructorArg(), (parser, context) -> parser.booleanValue(), SUCCEEDED, ObjectParser.ValueType.BOOLEAN);
        PARSER.declareField(constructorArg(), (parser, context) -> parser.intValue(), NUMFREED, ObjectParser.ValueType.INT);
    }

    private final boolean succeeded;
    private final int numFreed;

    public ClearScrollResponse(boolean succeeded, int numFreed) {
        this.succeeded = succeeded;
        this.numFreed = numFreed;
    }

    public ClearScrollResponse(StreamInput in) throws IOException {
        super(in);
        succeeded = in.readBoolean();
        numFreed = in.readVInt();
    }

    /**
     * @return Whether the attempt to clear a scroll was successful.
     */
    public boolean isSucceeded() {
        return succeeded;
    }

    /**
     * @return The number of search contexts that were freed. If this is <code>0</code> the assumption can be made,
     * that the scroll id specified in the request did not exist. (never existed, was expired, or completely consumed)
     */
    public int getNumFreed() {
        return numFreed;
    }

    @Override
    public RestStatus status() {
        return numFreed == 0 ? NOT_FOUND : OK;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SUCCEEDED.getPreferredName(), succeeded);
        builder.field(NUMFREED.getPreferredName(), numFreed);
        builder.endObject();
        return builder;
    }

    /**
     * Parse the clear scroll response body into a new {@link ClearScrollResponse} object
     */
    public static ClosePointInTimeResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(succeeded);
        out.writeVInt(numFreed);
    }
}
