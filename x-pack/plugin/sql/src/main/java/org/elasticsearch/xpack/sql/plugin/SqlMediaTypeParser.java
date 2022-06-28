/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.sql.action.SqlQueryRequest;
import org.elasticsearch.xpack.sql.proto.Mode;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.sql.proto.Protocol.URL_PARAM_FORMAT;

public class SqlMediaTypeParser {

    static class SqlMediaType {
        private final XContentType xContentType;
        private final TextFormat textFormat;
        private final boolean isTextFormat;

        private SqlMediaType(XContentType xContentType, TextFormat textFormat) {
            this.xContentType = xContentType;
            this.textFormat = textFormat;
            isTextFormat = textFormat != null;
        }

        private static SqlMediaType xContentType(XContentType xContentType) {
            return xContentType != null ? new SqlMediaType(xContentType, null) : null;
        }

        private static SqlMediaType textFormat(TextFormat textFormat) {
            return textFormat != null ? new SqlMediaType(null, textFormat) : null;
        }

        boolean isTextFormat() {
            return isTextFormat;
        }

        XContentType xContentType() {
            return xContentType;
        }

        TextFormat textFormat() {
            return textFormat;
        }

        private static SqlMediaType fromMediaTypeOrFormat(String mediaType) {
            XContentType xContentType = XContentType.fromMediaTypeOrFormat(mediaType);
            return xContentType != null ? xContentType(xContentType) : textFormat(TextFormat.fromMediaTypeOrFormat(mediaType));
        }
    }

    /*
     * Since we support {@link TextFormat} <strong>and</strong>
     * {@link XContent} outputs we can't use {@link RestToXContentListener}
     * like everything else. We want to stick as closely as possible to
     * Elasticsearch's defaults though, while still layering in ways to
     * control the output more easily.
     *
     * First we find the string that the user used to specify the response
     * format. If there is a {@code format} parameter we use that. If there
     * isn't but there is a {@code Accept} header then we use that. If there
     * isn't then we use the {@code Content-Type} header which is required.
     */
    public static SqlMediaType getResponseMediaType(RestRequest request, SqlQueryRequest sqlRequest) {
        if (Mode.isDedicatedClient(sqlRequest.requestInfo().mode())
            && (sqlRequest.binaryCommunication() == null || sqlRequest.binaryCommunication())) {
            // enforce CBOR response for drivers and CLI (unless instructed differently through the config param)
            return SqlMediaType.xContentType(XContentType.CBOR);
        } else if (request.hasParam(URL_PARAM_FORMAT)) {
            return validateColumnarRequest(sqlRequest.columnar(), mediaTypeFromParams(request), request);
        }

        return mediaTypeFromHeaders(request);
    }

    public static SqlMediaType getResponseMediaType(RestRequest request) {
        return request.hasParam(URL_PARAM_FORMAT)
            ? checkNonNullMediaType(mediaTypeFromParams(request), request)
            : mediaTypeFromHeaders(request);
    }

    private static SqlMediaType mediaTypeFromHeaders(RestRequest request) {
        String acceptType = getAcceptValue(request);
        SqlMediaType mediaType = acceptType != null
            ? SqlMediaType.fromMediaTypeOrFormat(acceptType)
            : SqlMediaType.xContentType(request.getXContentType());
        return checkNonNullMediaType(mediaType, request);
    }

    private static SqlMediaType mediaTypeFromParams(RestRequest request) {
        return SqlMediaType.fromMediaTypeOrFormat(request.param(URL_PARAM_FORMAT));
    }

    private static SqlMediaType validateColumnarRequest(boolean requestIsColumnar, SqlMediaType fromMediaType, RestRequest request) {
        if (requestIsColumnar && fromMediaType.isTextFormat()) {
            throw new IllegalArgumentException(
                "Invalid use of [columnar] argument: cannot be used in combination with " + "txt, csv or tsv formats"
            );
        }
        return checkNonNullMediaType(fromMediaType, request);
    }

    private static SqlMediaType checkNonNullMediaType(SqlMediaType mediaType, RestRequest request) {
        if (mediaType == null) {
            String msg = String.format(
                Locale.ROOT,
                "Invalid request content type: Accept=[%s], Content-Type=[%s], format=[%s]",
                request.header("Accept"),
                request.header("Content-Type"),
                request.param(URL_PARAM_FORMAT)
            );
            throw new IllegalArgumentException(msg);
        }

        return mediaType;
    }

    // Partially lifted from https://github.com/elastic/elasticsearch/pull/64406 RestRequest#parseHeaderWithMediaType()
    private static @Nullable String getAcceptValue(RestRequest request) {
        Map<String, List<String>> headers = request.getHeaders();
        final String headerName = "Accept";

        // TODO: make all usages of headers case-insensitive
        List<String> header = headers.get(headerName);
        if (header == null || header.isEmpty()) {
            return null;
        } else if (header.size() > 1) {
            throw new IllegalArgumentException("Incorrect header [" + headerName + "]. " + "Only one value should be provided");
        }
        String rawContentType = header.get(0);
        if (Strings.hasText(rawContentType)) {
            if ("*/*".equals(rawContentType)) {
                // */* means "I don't care" which we should treat like not specifying the header
                return null;
            }
            return rawContentType;
        } else {
            throw new IllegalArgumentException("Header [" + headerName + "] cannot be empty.");
        }

    }
}
