/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
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
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.formatter.TextFormat.CSV;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.URL_PARAM_DELIMITER;
import static org.elasticsearch.xpack.ql.util.LoggingUtils.logOnFailure;

/**
 * Listens for a single {@link EsqlQueryResponse}, builds a corresponding {@link RestResponse} and sends it.
 */
public class EsqlResponseListener extends RestResponseListener<EsqlQueryResponse> {
    /**
     * A simple, thread-safe stop watch for timing a single action.
     * Allows to stop the time for building a response and to log it at a later point.
     */
    private static class ThreadSafeStopWatch {
        /**
         * Start time of the watch
         */
        private final long startTimeNS = System.nanoTime();

        /**
         * End time of the watch
         */
        private long endTimeNS;

        /**
         * Is the stop watch currently running?
         */
        private boolean running = true;

        /**
         * Starts the {@link ThreadSafeStopWatch} immediately after construction.
         */
        ThreadSafeStopWatch() {}

        /**
         * Stop the stop watch (or do nothing if it was already stopped) and return the elapsed time since starting.
         * @return the elapsed time since starting the watch
         */
        public TimeValue stop() {
            synchronized (this) {
                if (running) {
                    endTimeNS = System.nanoTime();
                    running = false;
                }

                return new TimeValue(endTimeNS - startTimeNS, TimeUnit.NANOSECONDS);
            }
        }
    }

    private static final Logger LOGGER = LogManager.getLogger(EsqlResponseListener.class);
    private static final String HEADER_NAME_TOOK_NANOS = "Took-nanos";
    private final RestChannel channel;
    private final RestRequest restRequest;
    private final MediaType mediaType;
    /**
     * Keep the initial query for logging purposes.
     */
    private final String esqlQuery;
    /**
     * Stop the time it took to build a response to later log it. Use something thread-safe here because stopping time requires state and
     * {@link EsqlResponseListener} might be used from different threads.
     */
    private final ThreadSafeStopWatch stopWatch = new ThreadSafeStopWatch();

    /**
     * To correctly time the execution of a request, a {@link EsqlResponseListener} must be constructed immediately before execution begins.
     */
    public EsqlResponseListener(RestChannel channel, RestRequest restRequest, EsqlQueryRequest esqlRequest) {
        super(channel);

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
        boolean success = false;
        try {
            RestResponse restResponse;
            if (mediaType instanceof TextFormat format) {
                restResponse = RestResponse.chunked(
                    RestStatus.OK,
                    ChunkedRestResponseBody.fromTextChunks(
                        format.contentType(restRequest),
                        format.format(restRequest, esqlResponse),
                        esqlResponse
                    )
                );
            } else {
                restResponse = RestResponse.chunked(
                    RestStatus.OK,
                    ChunkedRestResponseBody.fromXContent(esqlResponse, channel.request(), channel, esqlResponse)
                );
            }
            long tookNanos = stopWatch.stop().getNanos();
            restResponse.addHeader(HEADER_NAME_TOOK_NANOS, Long.toString(tookNanos));
            success = true;
            return restResponse;
        } finally {
            if (success == false) {
                esqlResponse.close();
            }
        }
    }

    /**
     * Log the execution time and query when handling an ES|QL response.
     */
    public ActionListener<EsqlQueryResponse> wrapWithLogging() {
        return ActionListener.wrap(r -> {
            onResponse(r);
            // At this point, the StopWatch should already have been stopped, so we log a consistent time.
            LOGGER.info(
                "Finished execution of ESQL query.\nQuery string: [{}]\nExecution time: [{}]ms",
                esqlQuery,
                stopWatch.stop().getMillis()
            );
        }, ex -> {
            // In case of failure, stop the time manually before sending out the response.
            long timeMillis = stopWatch.stop().getMillis();
            LOGGER.info("Failed execution of ESQL query.\nQuery string: [{}]\nExecution time: [{}]ms", esqlQuery, timeMillis);
            logOnFailure(LOGGER, ex);
            onFailure(ex);
        });
    }
}
