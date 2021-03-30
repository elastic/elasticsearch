/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public final class OpenPointInTimeResponse extends ActionResponse implements ToXContentObject {
    private static final ParseField ID = new ParseField("id");

    private final String searchContextId;

    public OpenPointInTimeResponse(String searchContextId) {
        this.searchContextId = Objects.requireNonNull(searchContextId);
    }

    public OpenPointInTimeResponse(StreamInput in) throws IOException {
        super(in);
        searchContextId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(searchContextId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), searchContextId);
        builder.endObject();
        return builder;
    }

    public String getSearchContextId() {
        return searchContextId;
    }
}
