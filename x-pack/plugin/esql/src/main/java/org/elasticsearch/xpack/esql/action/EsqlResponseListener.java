/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;
import org.elasticsearch.xcontent.MediaType;
import org.elasticsearch.xpack.esql.arrow.ArrowFormat;
import org.elasticsearch.xpack.esql.arrow.ArrowResponse;
import org.elasticsearch.xpack.esql.formatter.TextFormat;
import org.elasticsearch.xpack.esql.plugin.EsqlMediaTypeParser;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.formatter.TextFormat.CSV;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.URL_PARAM_DELIMITER;

/**
 * Listens for a single {@link EsqlQueryResponse}, builds a corresponding {@link RestResponse} and sends it.
 */
public final class EsqlResponseListener extends RestRefCountedChunkedToXContentListener<EsqlQueryResponse> {
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

    /**
     * HTTP header names
     */
    public static final String HEADER_NAME_TOOK_NANOS = "Took-nanos";
    public static final String HEADER_NAME_ASYNC_ID = "Async-ID";
    public static final String HEADER_NAME_ASYNC_RUNNING = "Async-running";

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
    protected void processResponse(EsqlQueryResponse esqlQueryResponse) throws IOException {
        channel.sendResponse(buildResponse(esqlQueryResponse));
    }

    private RestResponse buildResponse(EsqlQueryResponse esqlResponse) throws IOException {
        boolean success = false;
        final Releasable releasable = releasableFromResponse(esqlResponse);
        try {
            RestResponse restResponse;
            if (mediaType instanceof TextFormat format) {
                restResponse = RestResponse.chunked(
                    RestStatus.OK,
                    ChunkedRestResponseBodyPart.fromTextChunks(format.contentType(restRequest), format.format(restRequest, esqlResponse)),
                    releasable
                );
                if (esqlResponse.asyncExecutionId().isPresent()) {
                    restResponse.addHeader(HEADER_NAME_ASYNC_ID, esqlResponse.asyncExecutionId().get());
                    restResponse.addHeader(HEADER_NAME_ASYNC_RUNNING, String.valueOf(esqlResponse.isRunning()));
                }
            } else if (mediaType == ArrowFormat.INSTANCE) {
                ArrowResponse arrowResponse = new ArrowResponse(
                    // Map here to avoid cyclic dependencies between the arrow subproject and its parent
                    esqlResponse.columns().stream().map(c -> new ArrowResponse.Column(c.outputType(), c.name())).toList(),
                    esqlResponse.pages()
                );
                restResponse = RestResponse.chunked(RestStatus.OK, arrowResponse, Releasables.wrap(arrowResponse, releasable));
            } else {
                restResponse = RestResponse.chunked(
                    RestStatus.OK,
                    ChunkedRestResponseBodyPart.fromXContent(esqlResponse, channel.request(), channel),
                    releasable
                );
            }
            long tookNanos = stopWatch.stop().getNanos();
            restResponse.addHeader(HEADER_NAME_TOOK_NANOS, Long.toString(tookNanos));
            success = true;
            return restResponse;
        } finally {
            if (success == false) {
                releasable.close();
            }
        }
    }

    /**
     * Log internal server errors all the time and log queries if debug is enabled.
     */
    public ActionListener<EsqlQueryResponse> wrapWithLogging() {
        ActionListener<EsqlQueryResponse> listener = ActionListener.wrap(this::onResponse, ex -> {
            logOnFailure(ex);
            onFailure(ex);
        });
        if (LOGGER.isDebugEnabled() == false) {
            return listener;
        }
        return ActionListener.wrap(r -> {
            listener.onResponse(r);
            // At this point, the StopWatch should already have been stopped, so we log a consistent time.
            LOGGER.debug(
                "Finished execution of ESQL query.\nQuery string: [{}]\nExecution time: [{}]ms",
                esqlQuery,
                stopWatch.stop().getMillis()
            );
        }, ex -> {
            // In case of failure, stop the time manually before sending out the response.
            long timeMillis = stopWatch.stop().getMillis();
            LOGGER.debug("Failed execution of ESQL query.\nQuery string: [{}]\nExecution time: [{}]ms", esqlQuery, timeMillis);
            listener.onFailure(ex);
        });
    }

    static void logOnFailure(Throwable throwable) {
        RestStatus status = ExceptionsHelper.status(throwable);
        LOGGER.log(status.getStatus() >= 500 ? Level.WARN : Level.DEBUG, () -> "Request failed with status [" + status + "]: ", throwable);
    }
}
