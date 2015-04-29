/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.http;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.WatcherException;

import java.io.IOException;

public class HttpResponse implements ToXContent {

    public static final ParseField STATUS_FIELD = new ParseField("status");
    public static final ParseField BODY_FIELD = new ParseField("body");

    private final int status;
    private final BytesReference body;

    public HttpResponse(int status) {
        this(status, (BytesReference) null);
    }

    public HttpResponse(int status, String body) {
        this(status, new BytesArray(body));
    }

    public HttpResponse(int status, byte[] body) {
        this(status, new BytesArray(body));
    }

    public HttpResponse(int status, BytesReference body) {
        this.status = status;
        this.body = body;
    }

    public int status() {
        return status;
    }

    public boolean hasContent() {
        return body != null;
    }

    public BytesReference body() {
        return body;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HttpResponse response = (HttpResponse) o;

        if (status != response.status) return false;
        return body.equals(response.body);

    }

    @Override
    public int hashCode() {
        int result = status;
        result = 31 * result + body.hashCode();
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder = builder.startObject().field(STATUS_FIELD.getPreferredName(), status);
        if (hasContent()) {
            builder = builder.field(BODY_FIELD.getPreferredName(), body.toUtf8());
        }
        builder.endObject();
        return builder;
    }

    public static HttpResponse parse(XContentParser parser) throws IOException {
        assert parser.currentToken() == XContentParser.Token.START_OBJECT;

        int status = -1;
        String body = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else {
                if (token == XContentParser.Token.VALUE_NUMBER) {
                    if (STATUS_FIELD.match(currentFieldName)) {
                        status = parser.intValue();
                    } else {
                        throw new ParseException("could not parse http response. unknown numeric field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (BODY_FIELD.match(currentFieldName)) {
                        body = parser.text();
                    } else {
                        throw new ParseException("could not parse http response. unknown string field [" + currentFieldName + "]");
                    }
                } else {
                    throw new ParseException("could not parse http response. unknown unexpected token [" + token + "]");
                }
            }
        }

        if (status < 0) {
            throw new ParseException("could not parse http response. missing [status] numeric field holding the response's http status code");
        }
        if (body == null) {
            return new HttpResponse(status);
        } else {
            return new HttpResponse(status, body);
        }
    }

    public static class ParseException extends WatcherException {

        public ParseException(String msg) {
            super(msg);
        }

        public ParseException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }
}