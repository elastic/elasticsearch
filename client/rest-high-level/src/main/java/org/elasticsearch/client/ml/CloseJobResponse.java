/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.common.xcontent.ParseField;
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
