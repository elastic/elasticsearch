/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.Nullable;
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

import static java.util.stream.Collectors.toSet;

public abstract class AbstractRestChannel implements RestChannel {

    private static final Logger logger = LogManager.getLogger(AbstractRestChannel.class);

    private static final Predicate<String> INCLUDE_FILTER = f -> f.charAt(0) != '-';
    private static final Predicate<String> EXCLUDE_FILTER = INCLUDE_FILTER.negate();

    protected final RestRequest request;
    private final boolean detailedErrorsEnabled;
    private final String format;
    private final String filterPath;
    private final boolean pretty;
    private final boolean human;
    private final String acceptHeader;

    private BytesStreamOutput bytesOut;

    /**
     * Construct a channel for handling the request.
     *
     * @param request               the request
     * @param detailedErrorsEnabled if detailed errors should be reported to the channel
     * @throws IllegalArgumentException if parsing the pretty or human parameters fails
     */
    protected AbstractRestChannel(RestRequest request, boolean detailedErrorsEnabled) {
        this.request = request;
        this.detailedErrorsEnabled = detailedErrorsEnabled;
        this.format = request.param("format");
        this.acceptHeader = request.header("Accept");
        this.filterPath = request.param("filter_path", null);
        this.pretty = request.paramAsBoolean("pretty", false);
        this.human = request.paramAsBoolean("human", false);
    }

    @Override
    public XContentBuilder newBuilder() throws IOException {
        return newBuilder(request.getXContentType(), true);
    }

    @Override
    public XContentBuilder newErrorBuilder() throws IOException {
        // release whatever output we already buffered and write error response to fresh buffer
        releaseOutputBuffer();
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
    @Override
    public XContentBuilder newBuilder(@Nullable XContentType requestContentType, boolean useFiltering) throws IOException {
        return newBuilder(requestContentType, null, useFiltering);
    }

    /**
     * Creates a new {@link XContentBuilder} for a response to be sent using this channel. The builder's type can be sent as a parameter,
     * through {@code responseContentType} or it can fallback to {@link #newBuilder(XContentType, boolean)} logic if the sent type value
     * is {@code null}.
     */
    @Override
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

        OutputStream unclosableOutputStream = Streams.flushOnCloseStream(bytesOutput());

        Map<String, String> parameters = request.getParsedAccept() != null ?
            request.getParsedAccept().getParameters() : Collections.emptyMap();
        ParsedMediaType responseMediaType = ParsedMediaType.parseMediaType(responseContentType, parameters);

        XContentBuilder builder =
            new XContentBuilder(XContentFactory.xContent(responseContentType), unclosableOutputStream,
                includes, excludes, responseMediaType, request.getRestApiVersion());
        if (pretty) {
            builder.prettyPrint().lfAtEnd();
        }

        builder.humanReadable(human);
        return builder;
    }

    /**
     * A channel level bytes output that can be reused. The bytes output is lazily instantiated
     * by a call to {@link #newBytesOutput()}. This method should only be called once per request.
     */
    @Override
    public final BytesStreamOutput bytesOutput() {
        if (bytesOut != null) {
            // fallback in case of encountering a bug, release the existing buffer if any (to avoid leaking memory) and acquire a new one
            // to send out an error response
            assert false : "getting here is always a bug";
            logger.error("channel handling [{}] reused", request.rawPath());
            releaseOutputBuffer();
        }
        bytesOut = newBytesOutput();
        return bytesOut;
    }

    /**
     * Releases the current output buffer for this channel. Must be called after the buffer derived from {@link #bytesOutput} is no longer
     * needed.
     */
    protected final void releaseOutputBuffer() {
        if (bytesOut != null) {
            bytesOut.close();
            bytesOut = null;
        }
    }

    protected BytesStreamOutput newBytesOutput() {
        return new BytesStreamOutput();
    }

    @Override
    public RestRequest request() {
        return this.request;
    }

    @Override
    public boolean detailedErrorsEnabled() {
        return detailedErrorsEnabled;
    }
}
