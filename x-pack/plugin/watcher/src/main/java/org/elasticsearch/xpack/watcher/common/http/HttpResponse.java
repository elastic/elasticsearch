/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.common.http;

import io.netty.handler.codec.http.HttpHeaders;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

public class HttpResponse implements ToXContentObject {

    private final int status;
    private final Map<String, String[]> headers;
    private final BytesReference body;

    public HttpResponse(int status) {
        this(status, emptyMap());
    }

    public HttpResponse(int status, Map<String, String[]> headers) {
        this(status, (BytesReference) null, headers);
    }

    public HttpResponse(int status, @Nullable String body) {
        this(status, body != null ? new BytesArray(body) : null, emptyMap());
    }

    public HttpResponse(int status, @Nullable String body, Map<String, String[]> headers) {
        this(status, body != null ? new BytesArray(body) : null, headers);
    }

    public HttpResponse(int status, @Nullable byte[] body) {
        this(status, body != null && body.length > 0 ? new BytesArray(body) : null, emptyMap());
    }

    public HttpResponse(int status, @Nullable byte[] body, Map<String, String[]> headers) {
        this(status, body != null && body.length > 0 ? new BytesArray(body) : null, headers);
    }

    public HttpResponse(int status, @Nullable BytesReference body, Map<String, String[]> headers) {
        this.status = status;
        this.body = body;
        this.headers = headers.entrySet()
                .stream()
                .collect(Collectors.toUnmodifiableMap(e -> e.getKey().toLowerCase(Locale.ROOT), Map.Entry::getValue));
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

    /**
     * Returns all the headers, with keys being lowercased, so they are always consistent
     * in the payload
     */
    public Map<String, List<String>> headers() {
        return headers.entrySet()
                .stream()
                .collect(Collectors.toUnmodifiableMap(e -> e.getKey().toLowerCase(Locale.ROOT), e -> Arrays.asList(e.getValue())));
    }

    public String[] header(String header) {
        return headers.get(header.toLowerCase(Locale.ROOT));
    }

    public String contentType() {
        String[] values = header(HttpHeaders.Names.CONTENT_TYPE);
        if (values == null || values.length == 0) {
            return null;
        }
        return values[0];
    }

    public XContentType xContentType() {
        String[] values = header(HttpHeaders.Names.CONTENT_TYPE);
        if (values == null || values.length == 0) {
            return null;
        }
        try {
            return XContentType.fromMediaType(values[0]);
        } catch (IllegalArgumentException e) {
            //HttpInputTests - content-type being unrecognized_content_type
            return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HttpResponse that = (HttpResponse) o;

        if (status != that.status) return false;
        if (headers.equals(that.headers) == false) return false;
        return Objects.equals(body, that.body);
    }

    @Override
    public int hashCode() {
        int result = status;
        result = 31 * result + headers.hashCode();
        result = 31 * result + (body != null ? body.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("status=[").append(status).append("]");
        if (headers.isEmpty() == false) {
            sb.append(", headers=[");
            boolean first = true;
            for (Map.Entry<String, String[]> header : headers.entrySet()) {
                if (first == false) {
                    sb.append(", ");
                }
                sb.append("[").append(header.getKey()).append(": ").append(Arrays.toString(header.getValue())).append("]");
                first = false;
            }
            sb.append("]");
        }
        if (hasContent()) {
            sb.append(", body=[").append(body.utf8ToString()).append("]");
        }
        return sb.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder = builder.startObject().field(Field.STATUS.getPreferredName(), status);
        if (headers.isEmpty() == false) {
            builder.startObject(Field.HEADERS.getPreferredName());
            for (Map.Entry<String, String[]> header : headers.entrySet()) {
                // in order to prevent dots in field names, that might occur in headers, we simply de_dot those header names
                // when writing toXContent
                builder.array(header.getKey().replaceAll("\\.", "_"), header.getValue());
            }
            builder.endObject();
        }
        if (hasContent()) {
            builder = builder.field(Field.BODY.getPreferredName(), body.utf8ToString());
        }
        builder.endObject();
        return builder;
    }

    public static HttpResponse parse(XContentParser parser) throws IOException {
        assert parser.currentToken() == XContentParser.Token.START_OBJECT;

        int status = -1;
        String body = null;
        Map<String, String[]> headers = new HashMap<>();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (currentFieldName == null) {
                throw new ElasticsearchParseException("could not parse http response. expected a field name but found [{}] instead", token);
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (Field.STATUS.match(currentFieldName, parser.getDeprecationHandler())) {
                    status = parser.intValue();
                } else {
                    throw new ElasticsearchParseException("could not parse http response. unknown numeric field [{}]", currentFieldName);
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (Field.BODY.match(currentFieldName, parser.getDeprecationHandler())) {
                    body = parser.text();
                } else {
                    throw new ElasticsearchParseException("could not parse http response. unknown string field [{}]", currentFieldName);
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                String headerName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        headerName = parser.currentName();
                    } else if (headerName == null){
                        throw new ElasticsearchParseException("could not parse http response. expected a header name but found [{}] " +
                                "instead", token);
                    } else if (token.isValue()) {
                        headers.put(headerName, new String[] { String.valueOf(parser.objectText()) });
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        List<String> values = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token.isValue() == false) {
                                throw new ElasticsearchParseException("could not parse http response. expected a header value for header " +
                                        "[{}] but found [{}] instead", headerName, token);
                            } else {
                                values.add(String.valueOf(parser.objectText()));
                            }
                        }
                        headers.put(headerName, values.toArray(new String[values.size()]));
                    }
                }
            } else {
                throw new ElasticsearchParseException("could not parse http response. unexpected token [{}]", token);
            }
        }

        if (status < 0) {
            throw new ElasticsearchParseException("could not parse http response. missing required numeric [{}] field holding the " +
                    "response's http status code", Field.STATUS.getPreferredName());
        }
        return new HttpResponse(status, body, unmodifiableMap(headers));
    }

    interface Field {
        ParseField STATUS = new ParseField("status");
        ParseField HEADERS = new ParseField("headers");
        ParseField BODY = new ParseField("body");
    }
}
