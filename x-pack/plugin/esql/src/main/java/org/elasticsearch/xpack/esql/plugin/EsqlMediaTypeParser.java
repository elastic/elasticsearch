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
import org.elasticsearch.xpack.esql.action.RestEsqlQueryAction;
import org.elasticsearch.xpack.esql.formatter.NdjsonFormat;
import org.elasticsearch.xpack.esql.formatter.TextFormat;
import org.elasticsearch.xpack.esql.formatter.arrow.ArrowFormat;

import java.util.Arrays;
import java.util.List;
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
        validateIncludeExecutionMetadata(esqlRequest.includeExecutionMetadata(), mediaType);
        validateProfile(esqlRequest.profile(), mediaType);
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
        String format = request.param(URL_PARAM_FORMAT);
        /*
         * NDJSON is not in MEDIA_TYPE_REGISTRY, see NdjsonFormat for why, so it is recognised here. It is released together
         * with streaming, so it shares that feature's gate: when the gate is closed this falls through to the registry, which
         * does not know "ndjson", and the request is rejected as an invalid format like any other unknown one.
         */
        if (RestEsqlQueryAction.STREAMING_ENABLED && NdjsonFormat.INSTANCE.queryParameter().equalsIgnoreCase(format)) {
            return NdjsonFormat.INSTANCE;
        }
        return MEDIA_TYPE_REGISTRY.queryParamToMediaType(format);
    }

    /**
     * Whether a response in this format can be produced incrementally, as the rows are computed ({@code streaming=true}).
     * This is the single place that decides it: a format that gains streaming support is added here.
     */
    public static boolean supportsStreaming(MediaType mediaType) {
        return mediaType == NdjsonFormat.INSTANCE;
    }

    /**
     * Whether batch boundaries are visible in the bytes of this format, which makes {@code batch_size} meaningful even when the
     * query is not streamed. Formats without record framing accept {@code batch_size} only together with {@code streaming=true}.
     */
    public static boolean hasRecordFraming(MediaType mediaType) {
        return mediaType == NdjsonFormat.INSTANCE;
    }

    private static void validateColumnarRequest(boolean requestIsColumnar, MediaType fromMediaType) {
        if (requestIsColumnar && fromMediaType instanceof TextFormat) {
            throw invalid("columnar");
        }
        if (requestIsColumnar && fromMediaType == NdjsonFormat.INSTANCE) {
            throw invalid("columnar", List.of(NdjsonFormat.INSTANCE.queryParameter()));
        }
    }

    private static void validateIncludeCCSMetadata(Boolean includeCCSMetadata, MediaType fromMediaType) {
        if (Boolean.TRUE.equals(includeCCSMetadata) && fromMediaType instanceof TextFormat) {
            throw invalid("include_ccs_metadata");
        }
    }

    private static void validateIncludeExecutionMetadata(Boolean includeExecutionMetadata, MediaType fromMediaType) {
        if (Boolean.TRUE.equals(includeExecutionMetadata) && fromMediaType instanceof TextFormat) {
            throw invalid("include_execution_metadata");
        }
    }

    private static void validateProfile(boolean profile, MediaType fromMediaType) {
        if (profile && fromMediaType instanceof TextFormat) {
            throw invalid("profile");
        }
    }

    private static IllegalArgumentException invalid(String argument) {
        return invalid(argument, Arrays.stream(TextFormat.values()).map(MediaType::queryParameter).toList());
    }

    private static IllegalArgumentException invalid(String argument, List<String> formats) {
        return new IllegalArgumentException(
            "Invalid use of [" + argument + "] argument: cannot be used in combination with " + formats + " formats"
        );
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
