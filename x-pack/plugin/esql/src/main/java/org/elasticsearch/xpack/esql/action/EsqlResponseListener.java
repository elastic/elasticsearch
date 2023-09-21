/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.rest.ChunkedRestResponseBody;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.xcontent.MediaType;
import org.elasticsearch.xpack.esql.formatter.TextFormat;
import org.elasticsearch.xpack.esql.plugin.EsqlMediaTypeParser;

import java.util.Locale;

import static org.elasticsearch.xpack.esql.formatter.TextFormat.CSV;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.URL_PARAM_DELIMITER;

/**
 * Listens for a single {@link EsqlQueryResponse}, builds a corresponding {@link RestResponse} and sends it.
 */
public class EsqlResponseListener extends RestResponseListener<EsqlQueryResponse> {
    private static final String HEADER_NAME_TOOK_NANOS = "Took-nanos";
    private final RestChannel channel;
    private final RestRequest restRequest;
    private final MediaType mediaType;
    /**
     * Stop the time it took to build a response to later log it. Use something thread-safe here because stopping time requires state and
     * {@link EsqlResponseListener} might be used from different threads.
     */
    private final ThreadSafeStopWatch responseTimeStopWatch;

    /**
     * Create a new listener which will send back a REST response for a single query.
     * @param channel the REST channel over which to send back the REST response
     * @param restRequest the initial REST resquest
     * @param esqlRequest the parsed ESQL request
     * @param responseTimeStopWatch a stopwatch that was started when we began handling the ESQL request; must be running and will be
     *                              stopped once the response is built
     */
    public EsqlResponseListener(
        RestChannel channel,
        RestRequest restRequest,
        EsqlQueryRequest esqlRequest,
        ThreadSafeStopWatch responseTimeStopWatch
    ) {
        super(channel);

        this.channel = channel;
        this.restRequest = restRequest;
        mediaType = EsqlMediaTypeParser.getResponseMediaType(restRequest, esqlRequest);
        this.responseTimeStopWatch = responseTimeStopWatch;

        /*
         * Special handling for the "delimiter" parameter which should only be
         * checked for being present or not in the case of CSV format. We cannot
         * override {@link BaseRestHandler#responseParams()} because this
         * parameter should only be checked for CSV, not other formats.
         */
        if (mediaType != CSV && restRequest.hasParam(URL_PARAM_DELIMITER)) {
            String message = String.format(
                Locale.ROOT,
                "parameter: [%s] can only be used with the format [%s] for request [%s]",
                URL_PARAM_DELIMITER,
                CSV.queryParameter(),
                restRequest.path()
            );
            throw new IllegalArgumentException(message);
        }
    }

    @Override
    public RestResponse buildResponse(EsqlQueryResponse esqlResponse) throws Exception {
        RestResponse restResponse;
        if (mediaType instanceof TextFormat format) {
            restResponse = RestResponse.chunked(
                RestStatus.OK,
                ChunkedRestResponseBody.fromTextChunks(format.contentType(restRequest), format.format(restRequest, esqlResponse))
            );
        } else {
            restResponse = RestResponse.chunked(
                RestStatus.OK,
                ChunkedRestResponseBody.fromXContent(esqlResponse, channel.request(), channel)
            );
        }
        long tookNanos = responseTimeStopWatch.stop().getNanos();
        restResponse.addHeader(HEADER_NAME_TOOK_NANOS, Long.toString(tookNanos));

        return restResponse;
    }
}
