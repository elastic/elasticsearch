/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
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
    private static final Logger LOGGER = LogManager.getLogger(EsqlResponseListener.class);
    private static final String HEADER_NAME_TOOK_NANOS = "Took-nanos";
    private final RestChannel channel;
    private final RestRequest restRequest;
    private final MediaType mediaType;
    /**
     * Keep the initial query for logging purposes.
     */
    private final String esqlQuery;
    private final StopWatch stopWatch = new StopWatch();

    /**
     * To correctly time the execution of a request, a {@link EsqlResponseListener} must be constructed immediately before execution begins.
     */
    public EsqlResponseListener(RestChannel channel, RestRequest restRequest, EsqlQueryRequest esqlRequest) {
        super(channel);

        stopWatch.start();

        this.channel = channel;
        this.restRequest = restRequest;
        this.esqlQuery = esqlRequest.query();
        mediaType = EsqlMediaTypeParser.getResponseMediaType(restRequest, esqlRequest);

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
        restResponse.addHeader(HEADER_NAME_TOOK_NANOS, Long.toString(stopTimeNanos()));

        return restResponse;
    }

    /**
     * Log the execution time and query when handling an ES|QL response.
     */
    public ActionListener<EsqlQueryResponse> wrapWithLogging() {
        return ActionListener.wrap(r -> {
            onResponse(r);
            // At this point, the StopWatch should already have been stopped, so we log a consistent time.
            LOGGER.info("Successfully executed ESQL query in {}ms:\n{}", stopTimeMillis(), esqlQuery);
        }, ex -> {
            // In case of failure, stop the time manually before sending out the response.
            long timeMillis = stopTimeMillis();
            onFailure(ex);
            LOGGER.info("Failed executing ESQL query in {}ms:\n{}", timeMillis, esqlQuery);
        });
    }

    /**
     * Stops {@link EsqlResponseListener#stopWatch} and returns the milliseconds elapsed since creating this {@link EsqlResponseListener}.
     * Can be safely called multiple times, even if {@link EsqlResponseListener#stopWatch} was already stopped.
     * @return the milliseconds since creation
     */
    private long stopTimeMillis() {
        safelyStopStopWatch();
        return stopWatch.totalTime().getMillis();
    }

    /**
     * Stops the internal {@link StopWatch} and returns the nanoseconds elapsed since creating this {@link EsqlResponseListener}.
     * Can be safely called multiple times, even if {@link EsqlResponseListener#stopWatch} was already stopped.
     * @return the nanoseconds since creation
     */
    private long stopTimeNanos() {
        safelyStopStopWatch();
        return stopWatch.totalTime().getNanos();
    }

    private void safelyStopStopWatch() {
        try {
            stopWatch.stop();
        } catch (IllegalStateException e) {
            // We do not care if the StopWatch was already stopped.
        }
    }
}
