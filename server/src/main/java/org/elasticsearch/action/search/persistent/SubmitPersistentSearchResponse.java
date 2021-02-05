/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search.persistent;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.persistent.PersistentSearchId;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownField;

public class SubmitPersistentSearchResponse extends ActionResponse implements StatusToXContentObject {
    private PersistentSearchId searchId;

    public SubmitPersistentSearchResponse(PersistentSearchId searchId) {
        this.searchId = searchId;
    }

    public SubmitPersistentSearchResponse(StreamInput in) throws IOException {
        super(in);
        this.searchId = PersistentSearchId.decode(in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(searchId.getEncodedId());
    }

    public PersistentSearchId getSearchId() {
        return searchId;
    }

    @Override
    public RestStatus status() {
        return RestStatus.ACCEPTED;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", searchId.getEncodedId());
        builder.endObject();
        return builder;
    }

    public static SubmitPersistentSearchResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        String fieldName = parser.currentName();
        if (fieldName.equals("id") == false) {
            throwUnknownField(fieldName, parser.getTokenLocation());
        }
        parser.nextToken();
        ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.currentToken(), parser);
        String id = parser.text();
        ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return new SubmitPersistentSearchResponse(PersistentSearchId.decode(id));
    }
}
