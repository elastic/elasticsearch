/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class ClosePointInTimeRequest extends ActionRequest implements ToXContentObject {
    private static final ParseField ID = new ParseField("id");

    private final String id;

    public ClosePointInTimeRequest(StreamInput in) throws IOException {
        super(in);
        this.id = in.readString();
    }

    public ClosePointInTimeRequest(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (Strings.isEmpty(id)) {
            throw new IllegalArgumentException("reader id must be specified");
        }
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        builder.endObject();
        return builder;
    }

    public static ClosePointInTimeRequest fromXContent(XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Malformed content, must start with an object");
        } else {
            XContentParser.Token token;
            String id = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME && parser.currentName().equals(ID.getPreferredName())) {
                    token = parser.nextToken();
                    if (token.isValue() == false) {
                        throw new IllegalArgumentException("the request must contain only [" + ID.getPreferredName() + " field");
                    }
                    id = parser.text();
                } else {
                    throw new IllegalArgumentException(
                        "Unknown parameter [" + parser.currentName() + "] in request body or parameter is of the wrong type[" + token + "] "
                    );
                }
            }
            if (Strings.isNullOrEmpty(id)) {
                throw new IllegalArgumentException("search context id is is not provided");
            }
            return new ClosePointInTimeRequest(id);
        }
    }
}
