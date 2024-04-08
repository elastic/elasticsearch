/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xcontent.MediaType;
import org.elasticsearch.xcontent.MediaTypeRegistry;
import org.elasticsearch.xcontent.ParsedMediaType;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.formatter.TextFormat;

import java.util.Arrays;
import java.util.Locale;

import static org.elasticsearch.xpack.esql.formatter.TextFormat.URL_PARAM_FORMAT;

public class EsqlMediaTypeParser {
    public static final MediaTypeRegistry<? extends MediaType> MEDIA_TYPE_REGISTRY = new MediaTypeRegistry<>().register(
        XContentType.values()
    ).register(TextFormat.values());

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
    public static MediaType getResponseMediaType(RestRequest request, EsqlQueryRequest esqlRequest) {
        var mediaType = request.hasParam(URL_PARAM_FORMAT) ? mediaTypeFromParams(request) : mediaTypeFromHeaders(request);
        return validateColumnarRequest(esqlRequest.columnar(), mediaType, request);
    }

    private static MediaType mediaTypeFromHeaders(RestRequest request) {
        ParsedMediaType acceptType = request.getParsedAccept();
        MediaType mediaType = acceptType != null ? acceptType.toMediaType(MEDIA_TYPE_REGISTRY) : request.getXContentType();
        return checkNonNullMediaType(mediaType, request);
    }

    private static MediaType mediaTypeFromParams(RestRequest request) {
        return MEDIA_TYPE_REGISTRY.queryParamToMediaType(request.param(URL_PARAM_FORMAT));
    }

    private static MediaType validateColumnarRequest(boolean requestIsColumnar, MediaType fromMediaType, RestRequest request) {
        if (requestIsColumnar && fromMediaType instanceof TextFormat) {
            throw new IllegalArgumentException(
                "Invalid use of [columnar] argument: cannot be used in combination with "
                    + Arrays.stream(TextFormat.values()).map(MediaType::queryParameter).toList()
                    + " formats"
            );
        }
        return checkNonNullMediaType(fromMediaType, request);
    }

    private static MediaType checkNonNullMediaType(MediaType mediaType, RestRequest request) {
        if (mediaType == null) {
            String msg = String.format(
                Locale.ROOT,
                "Invalid request content type: Accept=[%s], Content-Type=[%s], format=[%s]",
                request.header("Accept"),
                request.header("Content-Type"),
                request.param("format")
            );
            throw new IllegalArgumentException(msg);
        }

        return mediaType;
    }
}
