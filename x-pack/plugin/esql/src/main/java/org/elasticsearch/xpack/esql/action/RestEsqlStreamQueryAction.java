/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Build;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.URL_PARAM_FORMAT;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.URL_PARAM_HEADER;

/**
 * REST handler for {@code POST /_query/stream}: runs an ES|QL query and streams the result to the client as NDJSON
 * as it is produced, one row per line. See {@link EsqlStreamResponseListener} for the response shape.
 *
 * <p>The body is the same as for {@code POST /_query}, except that {@code columnar} is not supported, plus three fields
 * that only this API has: {@code max_rows} and {@code max_bytes}, which end the response early, and {@code store}, where
 * only {@code none} is supported. {@code max_rows} must be a whole JSON number. {@code max_bytes} is either a whole JSON
 * number, which is a count of bytes, or a size string with a unit such as {@code "1mb"}. The query runs only for as long
 * as the response is open: a disconnect cancels it, and so does a response that ends at a limit.</p>
 */
@ServerlessScope(Scope.PUBLIC)
public class RestEsqlStreamQueryAction extends BaseRestHandler {
    private static final Logger LOGGER = LogManager.getLogger(RestEsqlStreamQueryAction.class);

    static final String NDJSON_FORMAT_VALUE = "ndjson";
    static final String NDJSON_MEDIA_TYPE = "application/x-ndjson";
    static final String VND_NDJSON_MEDIA_TYPE = XContentType.VENDOR_APPLICATION_PREFIX + "x-ndjson";

    /**
     * Streaming is unreleased, so it is available on snapshot builds only. When this is false the handler is never
     * registered, which makes the endpoint indistinguishable from one that was never added.
     */
    public static final boolean ENABLED = Build.current().isSnapshot();

    private final EsqlCapabilities capabilities;

    private final BigArrays bigArrays;

    public RestEsqlStreamQueryAction(EsqlCapabilities capabilities, BigArrays bigArrays) {
        this.capabilities = capabilities;
        this.bigArrays = bigArrays;
    }

    @Override
    public String getName() {
        return "esql_stream_query";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_query/stream"));
    }

    @Override
    public Set<String> supportedCapabilities() {
        return capabilities.capabilities();
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        RequestXContent.ParsedStreamRequest parsed;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            parsed = RequestXContent.parseStream(parser);
        }
        return streamingChannelConsumer(parsed.request(), parsed.options(), request, client, bigArrays.bytesRefRecycler());
    }

    static RestChannelConsumer streamingChannelConsumer(
        EsqlQueryRequest esqlRequest,
        StreamOptions options,
        RestRequest request,
        NodeClient client,
        Recycler<BytesRef> recycler
    ) {
        if (esqlRequest.columnar()) {
            throw new IllegalArgumentException("[columnar] cannot be used with [POST /_query/stream]");
        }
        // format and header are read here, not in the channel consumer: BaseRestHandler rejects any parameter that
        // is still unconsumed once prepareRequest returns.
        validateFormat(request.param(URL_PARAM_FORMAT), request.header("Accept"));
        request.param(URL_PARAM_HEADER);

        final Boolean partialResults = request.paramAsBoolean("allow_partial_results", null);
        if (partialResults != null) {
            esqlRequest.allowPartialResults(partialResults);
        }
        LOGGER.debug("Beginning streaming execution of ESQL query.\nQuery string: [{}]", esqlRequest.queryDescription());

        return channel -> {
            EsqlStreamResponseListener restListener = new EsqlStreamResponseListener(
                channel,
                client.threadPool().getThreadContext(),
                recycler,
                client,
                options
            );
            EsqlStreamQueryRequest streamRequest = new EsqlStreamQueryRequest(
                esqlRequest,
                restListener.resultStreamListener(),
                request.paramAsBoolean(EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION, false),
                restListener::setPreHeaderFailureFooter
            );
            new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
                EsqlStreamQueryAction.INSTANCE,
                streamRequest,
                restListener
            );
        };
    }

    /**
     * NDJSON is the only format for now. It is the default, so a request may leave out both the {@code format} parameter
     * and the {@code Accept} header, but it may not ask for anything else. The REST API compatibility form of NDJSON
     * ({@code application/vnd.elasticsearch+x-ndjson;compatible-with=N}) is accepted too; the framework has already
     * validated the version by the time this runs. Quality values are honored: the most specific media range that covers
     * NDJSON decides, and {@code q=0} on it means NDJSON is not acceptable.
     */
    static void validateFormat(String format, String accept) {
        if (format != null && NDJSON_FORMAT_VALUE.equalsIgnoreCase(format) == false) {
            throw new IllegalArgumentException(
                "[format] must be [" + NDJSON_FORMAT_VALUE + "] for [POST /_query/stream], got [" + format + "]"
            );
        }
        if (accept == null || accept.isBlank()) {
            return;
        }
        int bestSpecificity = -1;
        double bestQuality = 0;
        for (String mediaRange : accept.split(",")) {
            String[] parts = mediaRange.split(";");
            int specificity = ndjsonSpecificity(parts[0].trim().toLowerCase(Locale.ROOT));
            if (specificity < 0) {
                continue;
            }
            double quality = parseQuality(parts, mediaRange);
            if (specificity > bestSpecificity) {
                bestSpecificity = specificity;
                bestQuality = quality;
            } else if (specificity == bestSpecificity) {
                bestQuality = Math.max(bestQuality, quality);
            }
        }
        if (bestSpecificity < 0 || bestQuality <= 0) {
            throw new IllegalArgumentException(
                "[Accept] must allow [" + NDJSON_MEDIA_TYPE + "] for [POST /_query/stream], got [" + accept + "]"
            );
        }
    }

    /**
     * How specifically a media range names NDJSON: 2 for an exact NDJSON type, 1 for {@code application/*},
     * 0 for {@code *}{@code /*}, and -1 if the range does not cover NDJSON.
     */
    private static int ndjsonSpecificity(String mediaType) {
        if (mediaType.equals(NDJSON_MEDIA_TYPE) || mediaType.equals(VND_NDJSON_MEDIA_TYPE)) {
            return 2;
        }
        if (mediaType.equals("application/*")) {
            return 1;
        }
        if (mediaType.equals("*/*")) {
            return 0;
        }
        return -1;
    }

    /** Reads the {@code q} parameter of a media range (parts[0] is the media type); defaults to 1. */
    private static double parseQuality(String[] parts, String mediaRange) {
        for (int i = 1; i < parts.length; i++) {
            String[] keyValue = parts[i].split("=", 2);
            if (keyValue.length == 2 && keyValue[0].trim().equalsIgnoreCase("q")) {
                double quality;
                try {
                    quality = Double.parseDouble(keyValue[1].trim());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("[Accept] has an invalid quality value in [" + mediaRange.trim() + "]");
                }
                if (Double.isNaN(quality) || quality < 0 || quality > 1) {
                    throw new IllegalArgumentException("[Accept] has an invalid quality value in [" + mediaRange.trim() + "]");
                }
                return quality;
            }
        }
        return 1.0;
    }

    @Override
    protected Set<String> responseParams() {
        return Set.of(URL_PARAM_FORMAT, URL_PARAM_HEADER, EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION);
    }
}
