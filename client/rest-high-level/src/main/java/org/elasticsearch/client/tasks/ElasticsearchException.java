/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.tasks;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * client side counterpart of server side
 * {@link org.elasticsearch.ElasticsearchException}
 * It wraps the same content but it is not throwable.
 */
public class ElasticsearchException {

    private static final String TYPE = "type";
    private static final String REASON = "reason";
    private static final String CAUSED_BY = "caused_by";
    private static final ParseField SUPPRESSED = new ParseField("suppressed");
    private static final String STACK_TRACE = "stack_trace";
    private static final String HEADER = "header";
    private static final String ROOT_CAUSE = "root_cause";

    private String msg;
    private ElasticsearchException cause;
    private final Map<String, List<String>> headers = new HashMap<>();
    private final List<ElasticsearchException> suppressed = new ArrayList<>();

    ElasticsearchException(String msg) {
        this.msg = msg;
        this.cause = null;
    }

    ElasticsearchException(String msg, ElasticsearchException cause) {
        this.msg = msg;
        this.cause = cause;
    }

    public String getMsg() {
        return msg;
    }

    public ElasticsearchException getCause() {
        return cause;
    }

    public List<ElasticsearchException> getSuppressed() {
        return suppressed;
    }

    void addSuppressed(List<ElasticsearchException> suppressed){
        this.suppressed.addAll(suppressed);
    }

    /**
     * Generate a {@link ElasticsearchException} from a {@link XContentParser}. This does not
     * return the original exception type (ie NodeClosedException for example) but just wraps
     * the type, the reason and the cause of the exception. It also recursively parses the
     * tree structure of the cause, returning it as a tree structure of {@link ElasticsearchException}
     * instances.
     */
    static ElasticsearchException fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
        return innerFromXContent(parser, false);
    }

    private static ElasticsearchException innerFromXContent(XContentParser parser, boolean parseRootCauses) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);

        String type = null, reason = null, stack = null;
        ElasticsearchException cause = null;
        Map<String, List<String>> headers = new HashMap<>();
        List<ElasticsearchException> rootCauses = new ArrayList<>();
        List<ElasticsearchException> suppressed = new ArrayList<>();

        for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
            String currentFieldName = parser.currentName();
            token = parser.nextToken();

            if (token.isValue()) {
                if (TYPE.equals(currentFieldName)) {
                    type = parser.text();
                } else if (REASON.equals(currentFieldName)) {
                    reason = parser.text();
                } else if (STACK_TRACE.equals(currentFieldName)) {
                    stack = parser.text();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (CAUSED_BY.equals(currentFieldName)) {
                    cause = fromXContent(parser);
                } else if (HEADER.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else {
                            List<String> values = headers.getOrDefault(currentFieldName, new ArrayList<>());
                            if (token == XContentParser.Token.VALUE_STRING) {
                                values.add(parser.text());
                            } else if (token == XContentParser.Token.START_ARRAY) {
                                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                    if (token == XContentParser.Token.VALUE_STRING) {
                                        values.add(parser.text());
                                    } else {
                                        parser.skipChildren();
                                    }
                                }
                            } else if (token == XContentParser.Token.START_OBJECT) {
                                parser.skipChildren();
                            }
                            headers.put(currentFieldName, values);
                        }
                    }
                } else {
                    // Any additional metadata object added by the metadataToXContent method is ignored
                    // and skipped, so that the parser does not fail on unknown fields. The parser only
                    // support metadata key-pairs and metadata arrays of values.
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (parseRootCauses && ROOT_CAUSE.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        rootCauses.add(fromXContent(parser));
                    }
                } else if (SUPPRESSED.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        suppressed.add(fromXContent(parser));
                    }
                } else {
                    // Parse the array and add each item to the corresponding list of metadata.
                    // Arrays of objects are not supported yet and just ignored and skipped.
                    List<String> values = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            values.add(parser.text());
                        } else {
                            parser.skipChildren();
                        }
                    }
                }
            }
        }

        ElasticsearchException e = new ElasticsearchException(buildMessage(type, reason, stack), cause);
        for (Map.Entry<String, List<String>> header : headers.entrySet()) {
            e.addHeader(header.getKey(), header.getValue());
        }

        // Adds root causes as suppressed exception. This way they are not lost
        // after parsing and can be retrieved using getSuppressed() method.
        e.suppressed.addAll(rootCauses);
        e.suppressed.addAll(suppressed);

        return e;
    }

    void addHeader(String key, List<String> value) {
        headers.put(key,value);

    }

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    static String buildMessage(String type, String reason, String stack) {
        StringBuilder message = new StringBuilder("Elasticsearch exception [");
        message.append(TYPE).append('=').append(type).append(", ");
        message.append(REASON).append('=').append(reason);
        if (stack != null) {
            message.append(", ").append(STACK_TRACE).append('=').append(stack);
        }
        message.append(']');
        return message.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof ElasticsearchException) == false) return false;
        ElasticsearchException that = (ElasticsearchException) o;
        return Objects.equals(getMsg(), that.getMsg()) &&
            Objects.equals(getCause(), that.getCause()) &&
            Objects.equals(getHeaders(), that.getHeaders()) &&
            Objects.equals(getSuppressed(), that.getSuppressed());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getMsg(), getCause(), getHeaders(), getSuppressed());
    }

    @Override
    public String toString() {
        return "ElasticsearchException{" +
            "msg='" + msg + '\'' +
            ", cause=" + cause +
            ", headers=" + headers +
            ", suppressed=" + suppressed +
            '}';
    }
}
