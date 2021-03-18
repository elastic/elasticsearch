/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.RestApiVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.ParsedMediaType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toSet;

public class XContentBuilderFactory {
    private static final Predicate<String> INCLUDE_FILTER = f -> f.charAt(0) != '-';
    private static final Predicate<String> EXCLUDE_FILTER = INCLUDE_FILTER.negate();

    private final RestRequest request;
    private final String format;
    private final String acceptHeader;
    private final String filterPath;
    private final boolean pretty;
    private final boolean human;
    private Supplier<BytesStreamOutput> bytesOutput;
    private RestApiVersion restApiVersion;

    public XContentBuilderFactory(RestRequest request, String format, String acceptHeader, String filterPath, boolean pretty, boolean human,
                                  Supplier<BytesStreamOutput> bytesOutput) {
        this(request, format, acceptHeader, filterPath, pretty, human, bytesOutput, RestApiVersion.current());
    }

    public XContentBuilderFactory(XContentBuilderFactory xContentBuilderFactory, RestApiVersion restApiVersion) {
        this(xContentBuilderFactory.request,
            xContentBuilderFactory.format,
            xContentBuilderFactory.acceptHeader,
            xContentBuilderFactory.filterPath,
            xContentBuilderFactory.pretty,
            xContentBuilderFactory.human,
            xContentBuilderFactory.bytesOutput,
            restApiVersion);
    }

    private XContentBuilderFactory(RestRequest request, String format, String acceptHeader, String filterPath, boolean pretty,
                                   boolean human, Supplier<BytesStreamOutput> bytesOutput, RestApiVersion restApiVersion) {
        this.request = request;
        this.format = format;
        this.acceptHeader = acceptHeader;
        this.filterPath = filterPath;
        this.pretty = pretty;
        this.human = human;
        this.bytesOutput = bytesOutput;
        this.restApiVersion = restApiVersion;
    }

    public XContentBuilder newBuilder() throws IOException {
        return newBuilder(request.getXContentType(), true);
    }

    public XContentBuilder newErrorBuilder() throws IOException {
        // Disable filtering when building error responses
        return newBuilder(request.getXContentType(), false);
    }

    /**
     * Creates a new {@link XContentBuilder} for a response to be sent using this channel. The builder's type is determined by the following
     * logic. If the request has a format parameter that will be used to attempt to map to an {@link XContentType}. If there is no format
     * parameter, the HTTP Accept header is checked to see if it can be matched to a {@link XContentType}. If this first attempt to map
     * fails, the request content type will be used if the value is not {@code null}; if the value is {@code null} the output format falls
     * back to JSON.
     */
    public XContentBuilder newBuilder(@Nullable XContentType requestContentType, boolean useFiltering) throws IOException {
        return newBuilder(requestContentType, null, useFiltering);
    }

    /**
     * Creates a new {@link XContentBuilder} for a response to be sent using this channel. The builder's type can be sent as a parameter,
     * through {@code responseContentType} or it can fallback to {@link #newBuilder(XContentType, boolean)} logic if the sent type value
     * is {@code null}.
     */
    public XContentBuilder newBuilder(@Nullable XContentType requestContentType, @Nullable XContentType responseContentType,
                                      boolean useFiltering) throws IOException {

        if (responseContentType == null) {
            if (Strings.hasText(format)) {
                responseContentType = XContentType.fromFormat(format);
            }
            if (responseContentType == null && Strings.hasText(acceptHeader)) {
                responseContentType = XContentType.fromMediaType(acceptHeader);
            }
        }
        // try to determine the response content type from the media type or the format query string parameter, with the format parameter
        // taking precedence over the Accept header
        if (responseContentType == null) {
            if (requestContentType != null) {
                // if there was a parsed content-type for the incoming request use that since no format was specified using the query
                // string parameter or the HTTP Accept header
                responseContentType = requestContentType;
            } else {
                // default to JSON output when all else fails
                responseContentType = XContentType.JSON;
            }
        }

        Set<String> includes = Collections.emptySet();
        Set<String> excludes = Collections.emptySet();
        if (useFiltering) {
            Set<String> filters = Strings.tokenizeByCommaToSet(filterPath);
            includes = filters.stream().filter(INCLUDE_FILTER).collect(toSet());
            excludes = filters.stream().filter(EXCLUDE_FILTER).map(f -> f.substring(1)).collect(toSet());
        }

        OutputStream unclosableOutputStream = Streams.flushOnCloseStream(bytesOutput.get());

        Map<String, String> parameters = request.getParsedAccept() != null ?
            request.getParsedAccept().getParameters() : Collections.emptyMap();
        ParsedMediaType responseMediaType = ParsedMediaType.parseMediaType(responseContentType, parameters);

        XContentBuilder builder =
            new XContentBuilder(XContentFactory.xContent(responseContentType), unclosableOutputStream,
                includes, excludes, responseMediaType, restApiVersion);
        if (pretty) {
            builder.prettyPrint().lfAtEnd();
        }

        builder.humanReadable(human);
        return builder;
    }
}
