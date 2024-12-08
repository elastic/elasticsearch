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
import org.elasticsearch.xpack.esql.arrow.ArrowFormat;
import org.elasticsearch.xpack.esql.formatter.TextFormat;

import java.util.Arrays;
import java.util.Locale;

import static org.elasticsearch.xpack.esql.formatter.TextFormat.URL_PARAM_FORMAT;

public class EsqlMediaTypeParser {
    public static final MediaTypeRegistry<? extends MediaType> MEDIA_TYPE_REGISTRY = new MediaTypeRegistry<>().register(
        XContentType.values()
    ).register(TextFormat.values()).register(new MediaType[] { ArrowFormat.INSTANCE });

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
     *
     * Also validates certain parameter combinations and throws IllegalArgumentException if invalid
     * combinations are detected.
     */
    public static MediaType getResponseMediaType(RestRequest request, EsqlQueryRequest esqlRequest) {
        var mediaType = getResponseMediaType(request, (MediaType) null);
        validateColumnarRequest(esqlRequest.columnar(), mediaType);
        validateIncludeCCSMetadata(esqlRequest.includeCCSMetadata(), mediaType);
        return checkNonNullMediaType(mediaType, request);
    }

    /*
     * Retrieve the mediaType of a REST request. If no mediaType can be established from the request, return the provided default.
     */
    public static MediaType getResponseMediaType(RestRequest request, MediaType defaultMediaType) {
        var mediaType = request.hasParam(URL_PARAM_FORMAT) ? mediaTypeFromParams(request) : mediaTypeFromHeaders(request);
        return mediaType == null ? defaultMediaType : mediaType;
    }

    private static MediaType mediaTypeFromHeaders(RestRequest request) {
        ParsedMediaType acceptType = request.getParsedAccept();
        return acceptType != null ? acceptType.toMediaType(MEDIA_TYPE_REGISTRY) : request.getXContentType();
    }

    private static MediaType mediaTypeFromParams(RestRequest request) {
        return MEDIA_TYPE_REGISTRY.queryParamToMediaType(request.param(URL_PARAM_FORMAT));
    }

    private static void validateColumnarRequest(boolean requestIsColumnar, MediaType fromMediaType) {
        if (requestIsColumnar && fromMediaType instanceof TextFormat) {
            throw new IllegalArgumentException(
                "Invalid use of [columnar] argument: cannot be used in combination with "
                    + Arrays.stream(TextFormat.values()).map(MediaType::queryParameter).toList()
                    + " formats"
            );
        }
    }

    private static void validateIncludeCCSMetadata(boolean includeCCSMetadata, MediaType fromMediaType) {
        if (includeCCSMetadata && fromMediaType instanceof TextFormat) {
            throw new IllegalArgumentException(
                "Invalid use of [include_ccs_metadata] argument: cannot be used in combination with "
                    + Arrays.stream(TextFormat.values()).map(MediaType::queryParameter).toList()
                    + " formats"
            );
        }
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
