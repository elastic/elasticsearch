/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

public class ClearScrollResponse extends ActionResponse implements ToXContentObject {

    public static final ParseField SUCCEEDED = new ParseField("succeeded");
    public static final ParseField NUMFREED = new ParseField("num_freed");

    private final boolean succeeded;
    private final int numFreed;

    public ClearScrollResponse(boolean succeeded, int numFreed) {
        this.succeeded = succeeded;
        this.numFreed = numFreed;
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(succeeded);
        out.writeVInt(numFreed);
    }
}
