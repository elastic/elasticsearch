/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.client.shared;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * A failure that happened on the remote server.
 */
public class RemoteFailure {
    private static final JsonFactory JSON_FACTORY = new JsonFactory();
    static {
        // Set up the factory similarly to how XContent does
        JSON_FACTORY.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
        JSON_FACTORY.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        JSON_FACTORY.configure(JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, true);
        // Do not automatically close unclosed objects/arrays in com.fasterxml.jackson.core.json.UTF8JsonGenerator#close() method
        JSON_FACTORY.configure(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT, false);
        JSON_FACTORY.configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, false);

    }
    public static RemoteFailure parseFromResponse(InputStream stream) throws IOException {
        try (JsonParser parser = JSON_FACTORY.createParser(stream)) {
            try {
                return parseResponseTopLevel(parser);
            } catch (IOException e) {
                throw new IOException(
                        "Can't parse error from Elasticearch [" + e.getMessage() + "] at [line "
                            + parser.getTokenLocation().getLineNr() + " col " + parser.getTokenLocation().getColumnNr() + "]",
                        e);
            }
        }
    }

    private final String type;
    private final String reason;
    private final String remoteTrace;
    private final Map<String, String> headers;
    private final RemoteFailure cause;

    RemoteFailure(String type, String reason, String remoteTrace, Map<String, String> headers, RemoteFailure cause) {
        this.type = type;
        this.reason = reason;
        this.remoteTrace = remoteTrace;
        this.headers = headers;
        this.cause = cause;
    }

    public String type() {
        return type;
    }

    public String reason() {
        return reason;
    }

    /**
     * Stack trace from Elasticsearch for the remote failure. Mostly just useful for debugging
     * errors that happen to be bugs.
     */
    public String remoteTrace() {
        return remoteTrace;
    }

    /**
     * Headers sent by the remote failure.
     */
    public Map<String, String> headers() {
        return headers;
    }

    /**
     * Cause of the remote failure. Mostly just useful for dbuegging errors that happen to be bugs.
     */
    public RemoteFailure cause() {
        return cause;
    }

    private static RemoteFailure parseResponseTopLevel(JsonParser parser) throws IOException {
        RemoteFailure exception = null;

        /* It'd be lovely to use the high level constructs that we have in core like ObjectParser
        * but, alas, we aren't going to modularize those out any time soon. */
        JsonToken token = parser.nextToken();
        if (token != JsonToken.START_OBJECT) {
            throw new IllegalArgumentException("Expected error to start with [START_OBJECT] but started with [" + token
                    + "][" + parser.getText() + "]");
        }
        String fieldName = null;
        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.FIELD_NAME) {
                fieldName = parser.getCurrentName();
            } else {
                switch (fieldName) {
                case "error":
                    if (token != JsonToken.START_OBJECT) {
                        throw new IOException("Expected [error] to be an object but was [" + token + "][" + parser.getText() + "]");
                    }
                    exception = parseFailure(parser);
                    continue;
                case "status":
                    if (token != JsonToken.VALUE_NUMBER_INT) {
                        throw new IOException("Expected [status] to be a string but was [" + token + "][" + parser.getText() + "]");
                    }
                    // Intentionally ignored
                    continue;
                default:
                    throw new IOException("Expected one of [error, status] but got [" + fieldName + "][" + parser.getText() + "]");
                }
            }
        }
        if (exception == null) {
            throw new IOException("Expected [error] but didn't see it.");
        }
        return exception;
    }

    private static RemoteFailure parseFailure(JsonParser parser) throws IOException {
        String type = null;
        String reason = null;
        String remoteTrace = null;
        Map<String, String> headers = emptyMap();
        RemoteFailure cause = null;

        JsonToken token;
        String fieldName = null;
        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.FIELD_NAME) {
                fieldName = parser.getCurrentName();
            } else {
                switch (fieldName) {
                case "caused_by":
                    if (token != JsonToken.START_OBJECT) {
                        throw new IOException("Expected [caused_by] to be an object but was [" + token + "][" + parser.getText() + "]");
                    }
                    cause = parseFailure(parser);
                    break;
                case "header":
                    if (token != JsonToken.START_OBJECT) {
                        throw new IOException("Expected [header] to be an object but was [" + token + "][" + parser.getText() + "]");
                    }
                    headers = parseHeaders(parser);
                    break;
                case "reason":
                    switch (token) {
                    case VALUE_STRING:
                        reason = parser.getText();
                        break;
                    case VALUE_NULL:
                        break;
                    default:
                        throw new IOException("Expected [reason] to be a string but was [" + token + "][" + parser.getText() + "]");
                    }
                    break;
                case "root_cause":
                    if (token != JsonToken.START_ARRAY) {
                        throw new IOException("Expected [root_cause] to be an array but was [" + token + "][" + parser.getText() + "]");
                    }
                    parser.skipChildren();   // Intentionally ignored
                    break;
                case "stack_trace":
                    if (token != JsonToken.VALUE_STRING) {
                        throw new IOException("Expected [stack_trace] to be a string but was [" + token + "][" + parser.getText() + "]");
                    }
                    remoteTrace = parser.getText();
                    break;
                case "type":
                    if (token != JsonToken.VALUE_STRING) {
                        throw new IOException("Expected [type] to be a string but was [" + token + "][" + parser.getText() + "]");
                    }
                    type = parser.getText();
                    break;
                default:
                    throw new IOException("Expected one of [caused_by, reason, root_cause, stack_trace, type] but got ["
                            + fieldName + "]");
                }
            }
        }
        if (type == null) {
            throw new IOException("expected [type] but didn't see it");
        }
        if (remoteTrace == null) {
            throw new IOException("expected [stack_trace] cannot but didn't see it");
        }
        return new RemoteFailure(type, reason, remoteTrace, headers, cause);
    }

    private static Map<String, String> parseHeaders(JsonParser parser) throws IOException {
        Map<String, String> headers = new HashMap<>();

        JsonToken token;
        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
            if (token != JsonToken.FIELD_NAME) {
                throw new IOException("expected header name but was [" + token + "][" + parser.getText() + "]");
            }
            String name = parser.getText();
            token = parser.nextToken();
            if (token != JsonToken.VALUE_STRING) {
                throw new IOException("expected header value but was [" + token + "][" + parser.getText() + "]");
            }
            String value = parser.getText();
            headers.put(name, value);
        }

        return headers;
    }
}
