/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import com.google.protobuf.MessageLite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractOTLPTransportAction extends HandledTransportAction<OTLPActionRequest, OTLPActionResponse> {

    private static final Logger logger = LogManager.getLogger(AbstractOTLPTransportAction.class);
    public static final int IGNORED_DATA_POINTS_MESSAGE_LIMIT = 10;
    private final Client client;

    @Inject
    public AbstractOTLPTransportAction(
        String name,
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client
    ) {
        super(name, transportService, actionFilters, OTLPActionRequest::new, threadPool.executor(ThreadPool.Names.WRITE));
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, OTLPActionRequest request, ActionListener<OTLPActionResponse> listener) {
        ProcessingContext context = ProcessingContext.EMPTY;
        try {
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            context = prepareBulkRequest(request, bulkRequestBuilder);
            if (bulkRequestBuilder.numberOfActions() == 0) {
                if (context.getIgnoredDataPoints() == 0) {
                    handleEmptyRequest(listener);
                } else {
                    // all data points were ignored
                    handlePartialSuccess(listener, context);
                }
                return;
            }

            ProcessingContext finalContext = context;
            bulkRequestBuilder.execute(new ActionListener<>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                    if (bulkItemResponses.hasFailures() || finalContext.getIgnoredDataPoints() > 0) {
                        handlePartialSuccess(bulkItemResponses, finalContext, listener);
                    } else {
                        handleSuccess(listener);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    handleFailure(listener, e, finalContext);
                }
            });

        } catch (Exception e) {
            logger.error("failed to execute otlp request", e);
            handleFailure(listener, e, context);
        }
    }

    /**
     * Tracks the outcome of processing an OTLP export request, including the total number of items processed,
     * how many were ignored, and any associated error messages. This information is used to build
     * OTLP partial success and failure responses.
     *
     * @see <a href="https://opentelemetry.io/docs/specs/otlp/#partial-success-1">OTLP Partial Success</a>
     */
    public interface ProcessingContext {

        ProcessingContext EMPTY = () -> 0;

        /**
         * Creates a ProcessingContext that only tracks the total number of data points processed
         * and does not track any ignored data points or error messages.
         *
         * @param totalDataPoints the total number of data points processed
         * @return a ProcessingContext instance with the specified total data points and no ignored data points or error messages
         */
        static ProcessingContext withTotalDataPoints(int totalDataPoints) {
            return new WithTotalDataPoints(totalDataPoints);
        }

        int totalDataPoints();

        default int getIgnoredDataPoints() {
            return 0;
        }

        default String getIgnoredDataPointsMessage(int limit) {
            return "";
        }

        /**
         * A simple implementation of ProcessingContext that only tracks the total number of data points processed
         * and does not track any ignored data points or error messages.
         */
        record WithTotalDataPoints(int totalDataPoints) implements ProcessingContext {}
    }

    /**
     * Parses the OTLP export request and populates the given bulk request builder with the corresponding index operations.
     *
     * @param request            the incoming OTLP export request
     * @param bulkRequestBuilder the bulk request builder to populate with index operations
     * @return a {@link ProcessingContext} summarizing the outcome, including total and rejected item counts
     * @throws IOException if creating the source for the bulk request fails
     */
    protected abstract ProcessingContext prepareBulkRequest(OTLPActionRequest request, BulkRequestBuilder bulkRequestBuilder)
        throws IOException;

    public void handleSuccess(ActionListener<OTLPActionResponse> listener) {
        listener.onResponse(new OTLPActionResponse(RestStatus.OK, BytesArray.EMPTY));
    }

    public void handleEmptyRequest(ActionListener<OTLPActionResponse> listener) {
        // If the server receives an empty request
        // (a request that does not carry any telemetry data)
        // the server SHOULD respond with success.
        // https://opentelemetry.io/docs/specs/otlp/#full-success-1
        handleSuccess(listener);
    }

    public void handlePartialSuccess(ActionListener<OTLPActionResponse> listener, ProcessingContext context) {
        // If the request is only partially accepted
        // (i.e. when the server accepts only parts of the data and rejects the rest),
        // the server MUST respond with HTTP 200 OK.
        // https://opentelemetry.io/docs/specs/otlp/#partial-success-1
        MessageLite response = responseWithRejectedDataPoints(
            context.getIgnoredDataPoints(),
            context.getIgnoredDataPointsMessage(IGNORED_DATA_POINTS_MESSAGE_LIMIT)
        );
        listener.onResponse(new OTLPActionResponse(RestStatus.BAD_REQUEST, response));
    }

    private void handlePartialSuccess(
        BulkResponse bulkItemResponses,
        ProcessingContext context,
        ActionListener<OTLPActionResponse> listener
    ) {
        // index -> status -> failure group
        Map<String, Map<RestStatus, FailureGroup>> failureGroups = new HashMap<>();
        // If the request is only partially accepted
        // (i.e. when the server accepts only parts of the data and rejects the rest),
        // the server MUST respond with HTTP 200 OK.
        // https://opentelemetry.io/docs/specs/otlp/#partial-success-1
        RestStatus status = RestStatus.OK;
        int failures = 0;
        for (BulkItemResponse bulkItemResponse : bulkItemResponses.getItems()) {
            BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
            if (failure != null) {
                // we're counting each document as one data point here
                // which is an approximation since one document can represent multiple data points
                failures++;
                if (failure.getStatus() == RestStatus.TOO_MANY_REQUESTS) {
                    // If the server receives more requests than the client is allowed or the server is overloaded,
                    // the server SHOULD respond with HTTP 429 Too Many Requests or HTTP 503 Service Unavailable
                    // and MAY include “Retry-After” header with a recommended time interval in seconds to wait before retrying.
                    // https://opentelemetry.io/docs/specs/otlp/#otlphttp-throttling
                    status = RestStatus.TOO_MANY_REQUESTS;
                }
                FailureGroup failureGroup = failureGroups.computeIfAbsent(failure.getIndex(), k -> new HashMap<>())
                    .computeIfAbsent(failure.getStatus(), k -> new FailureGroup(new AtomicInteger(0), failure.getMessage()));
                failureGroup.failureCount().incrementAndGet();
            }
        }
        if (bulkItemResponses.getItems().length == failures) {
            // all data points failed, so we report total data points as failures
            failures = context.totalDataPoints();
        }
        StringBuilder failureMessageBuilder = new StringBuilder();
        for (Map.Entry<String, Map<RestStatus, FailureGroup>> indexEntry : failureGroups.entrySet()) {
            String indexName = indexEntry.getKey();
            for (Map.Entry<RestStatus, FailureGroup> statusEntry : indexEntry.getValue().entrySet()) {
                RestStatus restStatus = statusEntry.getKey();
                FailureGroup failureGroup = statusEntry.getValue();
                failureMessageBuilder.append("Index [")
                    .append(indexName)
                    .append("] returned status [")
                    .append(restStatus)
                    .append("] for ")
                    .append(failureGroup.failureCount())
                    .append(" documents. Sample error message: ");
                failureMessageBuilder.append(failureGroup.failureMessageSample());
                failureMessageBuilder.append("\n");
            }
        }
        failureMessageBuilder.append(context.getIgnoredDataPointsMessage(10));
        MessageLite response = responseWithRejectedDataPoints(failures + context.getIgnoredDataPoints(), failureMessageBuilder.toString());
        listener.onResponse(new OTLPActionResponse(status, response));
    }

    record FailureGroup(AtomicInteger failureCount, String failureMessageSample) {}

    public void handleFailure(ActionListener<OTLPActionResponse> listener, Exception e, ProcessingContext context) {
        // https://opentelemetry.io/docs/specs/otlp/#failures-1
        // If the processing of the request fails,
        // the server MUST respond with appropriate HTTP 4xx or HTTP 5xx status code.
        listener.onResponse(
            new OTLPActionResponse(ExceptionsHelper.status(e), responseWithRejectedDataPoints(context.totalDataPoints(), e.getMessage()))
        );
    }

    /**
     * Builds the response for a request that had some rejected data points.
     *
     * @param rejectedDataPoints the number of data points that were rejected
     * @param message            a message describing the reason for rejection, which may be included in the response body
     * @return a MessageLite containing the response message with details about the rejected data points
     */
    abstract MessageLite responseWithRejectedDataPoints(int rejectedDataPoints, String message);

}
