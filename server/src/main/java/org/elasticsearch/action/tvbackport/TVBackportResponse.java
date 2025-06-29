/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.tvbackport;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.versions.TestTV;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class TVBackportResponse extends ActionResponse implements ToXContentObject {
    String message;
    String testTV;

    public TVBackportResponse(String message, String testTV) {
        this.message = message;
        this.testTV = testTV;
    }

    public TVBackportResponse(StreamInput in) throws IOException {
        if (TestTV.INSTANCE.isCompatible(in.getTransportVersion())) {
            this.testTV = in.readOptionalString();
        } else {
            this.testTV = null; // Not compatible, so we don't read it
        }
        this.message = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (TestTV.INSTANCE.isCompatible(out.getTransportVersion())) {
            out.writeOptionalString(testTV);
        }
        out.writeString(message);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (testTV != null) {
            builder.field("TestTV", testTV);
        }
        builder.field("message", message);
        builder.endObject();
        return builder;
    }
}
