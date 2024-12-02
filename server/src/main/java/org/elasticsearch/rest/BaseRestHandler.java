/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.apache.lucene.search.spell.LevenshteinDistance;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.action.admin.cluster.RestNodesUsageAction;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * Base handler for REST requests.
 * <p>
 * This handler makes sure that the headers &amp; context of the handled {@link RestRequest requests} are copied over to
 * the transport requests executed by the associated client. While the context is fully copied over, not all the headers
 * are copied, but a selected few. It is possible to control what headers are copied over by returning them in
 * {@link ActionPlugin#getRestHeaders()}.
 */
public abstract class BaseRestHandler implements RestHandler {

    public static final Setting<Boolean> MULTI_ALLOW_EXPLICIT_INDEX = Setting.boolSetting(
        "rest.action.multi.allow_explicit_index",
        true,
        Property.NodeScope
    );

    private final LongAdder usageCount = new LongAdder();

    public final long getUsageCount() {
        return usageCount.sum();
    }

    /**
     * @return the name of this handler. The name should be human readable and
     *         should describe the action that will performed when this API is
     *         called. This name is used in the response to the
     *         {@link RestNodesUsageAction}.
     */
    public abstract String getName();

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract List<Route> routes();

    private static final Set<String> ALWAYS_SUPPORTED = Set.of("format", "filter_path", "pretty", "human");

    @Override
    public final void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        // check if the query has any parameters that are not in the supported set (if declared)
        Set<String> supported = allSupportedParameters();
        assert supported == allSupportedParameters() : getName() + ": did not return same instance from allSupportedParameters()";
        if (supported != null) {
            var allSupported = Sets.union(
                RestResponse.RESPONSE_PARAMS,
                ALWAYS_SUPPORTED,
                // these internal parameters cannot be set by end-users, but are used by Elasticsearch internally.
                // they must be accepted by all handlers
                RestRequest.INTERNAL_MARKER_REQUEST_PARAMETERS,
                supported
            );
            if (allSupported.containsAll(request.params().keySet()) == false) {
                Set<String> unsupported = Sets.difference(request.params().keySet(), allSupported);
                throw new IllegalArgumentException(unrecognized(request, unsupported, allSupported, "parameter"));
            }
        }

        // prepare the request for execution; has the side effect of touching the request parameters
        try (var action = prepareRequest(request, client)) {

            assert assertConsumesSupportedParams(supported, request);

            // validate unconsumed params, but we must exclude params used to format the response
            // use a sorted set so the unconsumed parameters appear in a reliable sorted order
            final SortedSet<String> unconsumedParams = request.unconsumedParams()
                .stream()
                .filter(p -> RestResponse.RESPONSE_PARAMS.contains(p) == false)
                .filter(p -> responseParams(request.getRestApiVersion()).contains(p) == false)
                .collect(Collectors.toCollection(TreeSet::new));

            // validate the non-response params
            if (unconsumedParams.isEmpty() == false) {
                final Set<String> candidateParams = new HashSet<>();
                candidateParams.addAll(request.consumedParams());
                candidateParams.addAll(responseParams(request.getRestApiVersion()));
                throw new IllegalArgumentException(unrecognized(request, unconsumedParams, candidateParams, "parameter"));
            }

            if (request.hasContent() && (request.isContentConsumed() == false && request.isFullContent())) {
                throw new IllegalArgumentException(
                    "request [" + request.method() + " " + request.path() + "] does not support having a body"
                );
            }

            usageCount.increment();
            if (request.isStreamedContent()) {
                assert action instanceof RequestBodyChunkConsumer;
                var chunkConsumer = (RequestBodyChunkConsumer) action;

                request.contentStream().setHandler(new HttpBody.ChunkHandler() {
                    @Override
                    public void onNext(ReleasableBytesReference chunk, boolean isLast) {
                        chunkConsumer.handleChunk(channel, chunk, isLast);
                    }

                    @Override
                    public void close() {
                        chunkConsumer.streamClose();
                    }
                });
                action.accept(channel);
            } else {
                action.accept(channel);
                request.getHttpRequest().release();
            }
        }
    }

    private boolean assertConsumesSupportedParams(@Nullable Set<String> supported, RestRequest request) {
        if (supported != null) {
            final var supportedAndCommon = new TreeSet<>(supported);
            supportedAndCommon.add("error_trace");
            supportedAndCommon.addAll(ALWAYS_SUPPORTED);
            supportedAndCommon.removeAll(RestRequest.INTERNAL_MARKER_REQUEST_PARAMETERS);
            final var consumed = new TreeSet<>(request.consumedParams());
            consumed.removeAll(RestRequest.INTERNAL_MARKER_REQUEST_PARAMETERS);
            assert supportedAndCommon.equals(consumed)
                : getName() + ": consumed params " + consumed + " while supporting " + supportedAndCommon;
        }
        return true;
    }

    protected static String unrecognized(RestRequest request, Set<String> invalids, Set<String> candidates, String detail) {
        StringBuilder message = new StringBuilder().append("request [")
            .append(request.path())
            .append("] contains unrecognized ")
            .append(detail)
            .append(invalids.size() > 1 ? "s" : "")
            .append(": ");

        for (Iterator<String> it = invalids.iterator(); it.hasNext();) {
            String invalid = it.next();

            LevenshteinDistance ld = new LevenshteinDistance();
            List<String> candidateParams = candidates.stream()
                .map(c -> Tuple.tuple(ld.getDistance(invalid, c), c))
                .filter(t -> t.v1() > 0.5f)
                .sorted(Comparator.<Tuple<Float, String>, Float>comparing(Tuple::v1).reversed().thenComparing(Tuple::v2))
                .map(Tuple::v2)
                .toList();

            message.append("[").append(invalid).append("]");
            if (candidateParams.isEmpty() == false) {
                message.append(" -> did you mean ");
                if (candidateParams.size() > 1) {
                    message.append("any of ");
                }
                message.append(candidateParams);
                message.append("?");
            }

            if (it.hasNext()) {
                message.append(", ");
            }
        }

        return message.toString();
    }

    /**
     * REST requests are handled by preparing a channel consumer that represents the execution of the request against a channel.
     */
    @FunctionalInterface
    protected interface RestChannelConsumer extends CheckedConsumer<RestChannel, Exception>, Releasable {
        /**
         * Called just after the execution has started (or failed, if the request was invalid), but typically well before the execution has
         * completed. This callback should be used to release (refs to) resources that were acquired when constructing this consumer, for
         * instance by calling {@link RefCounted#decRef()} on any newly-created transport requests with nontrivial lifecycles.
         */
        @Override
        default void close() {}
    }

    public interface RequestBodyChunkConsumer extends RestChannelConsumer {

        /**
         * Handle one chunk of the request body. The handler <b>must</b> close the chunk once it is no longer
         * needed to avoid leaking.
         *
         * @param channel The rest channel associated to the request
         * @param chunk The chunk of request body that is ready for processing
         * @param isLast Whether the chunk is the last one of the request
         */
        void handleChunk(RestChannel channel, ReleasableBytesReference chunk, boolean isLast);

        /**
         * Called when the stream closes. This could happen prior to the completion of the request if the underlying channel was closed.
         * Implementors should do their best to clean up resources and early terminate request processing if it is triggered before a
         * response is generated.
         */
        default void streamClose() {}
    }

    /**
     * Prepare the request for execution. Implementations should consume all request params before
     * returning the runnable for actual execution. Unconsumed params will immediately terminate
     * execution of the request. However, some params are only used in processing the response;
     * implementations can override {@link BaseRestHandler#responseParams()} to indicate such
     * params.
     *
     * @param request the request to execute
     * @param client  client for executing actions on the local node
     * @return the action to execute
     * @throws IOException if an I/O exception occurred parsing the request and preparing for
     *                     execution
     */
    protected abstract RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException;

    /**
     * Parameters used for controlling the response and thus might not be consumed during
     * preparation of the request execution in
     * {@link BaseRestHandler#prepareRequest(RestRequest, NodeClient)}.
     *
     * @return a set of parameters used to control the response and thus should not trip strict
     * URL parameter checks.
     */
    protected Set<String> responseParams() {
        return Collections.emptySet();
    }

    /**
     * Parameters used for controlling the response and thus might not be consumed during
     * preparation of the request execution. The value depends on the RestApiVersion provided
     * by a user on a request.
     * Used in RestHandlers with Compatible Rest Api
     * @param restApiVersion - a version provided by a user on a request
     * @return a set of parameters used to control the response, depending on a restApiVersion
     */
    protected Set<String> responseParams(RestApiVersion restApiVersion) {
        return responseParams();
    }

}
