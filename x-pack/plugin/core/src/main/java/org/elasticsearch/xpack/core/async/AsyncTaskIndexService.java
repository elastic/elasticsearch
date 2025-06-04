/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Streams;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.SearchStatusResponse;
import org.elasticsearch.xpack.core.security.SecurityContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.CharBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.search.SearchService.MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;

/**
 * A service that exposes the CRUD operations for the async task-specific index.
 */
public final class AsyncTaskIndexService<R extends AsyncResponse<R>> {
    private static final Logger logger = LogManager.getLogger(AsyncTaskIndexService.class);

    public static final String HEADERS_FIELD = "headers";
    public static final String RESPONSE_HEADERS_FIELD = "response_headers";
    public static final String EXPIRATION_TIME_FIELD = "expiration_time";
    public static final String RESULT_FIELD = "result";
    private static final int ASYNC_TASK_INDEX_MAPPINGS_VERSION = 0;

    // Usually the settings, mappings and system index descriptor below
    // would be co-located with the SystemIndexPlugin implementation,
    // however in this case this service is in a different project to
    // AsyncResultsIndexPlugin, as are tests that need access to
    // #settings().

    static Settings settings() {
        return Settings.builder()
            .put("index.codec", "best_compression")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .build();
    }

    private static XContentBuilder mappings() {
        try {
            XContentBuilder builder = jsonBuilder().startObject()
                .startObject(SINGLE_MAPPING_NAME)
                .startObject("_meta")
                .field("version", Version.CURRENT)
                .field(SystemIndexDescriptor.VERSION_META_KEY, ASYNC_TASK_INDEX_MAPPINGS_VERSION)
                .endObject()
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject(HEADERS_FIELD)
                .field("type", "object")
                .field("enabled", "false")
                .endObject()
                .startObject(RESPONSE_HEADERS_FIELD)
                .field("type", "object")
                .field("enabled", "false")
                .endObject()
                .startObject(RESULT_FIELD)
                .field("type", "object")
                .field("enabled", "false")
                .endObject()
                .startObject(EXPIRATION_TIME_FIELD)
                .field("type", "long")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
            return builder;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build mappings for " + XPackPlugin.ASYNC_RESULTS_INDEX, e);
        }
    }

    public static SystemIndexDescriptor getSystemIndexDescriptor() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(XPackPlugin.ASYNC_RESULTS_INDEX + "*")
            .setDescription("Async search results")
            .setPrimaryIndex(XPackPlugin.ASYNC_RESULTS_INDEX)
            .setMappings(mappings())
            .setSettings(settings())
            .setOrigin(ASYNC_SEARCH_ORIGIN)
            .build();
    }

    private final String index;
    private final ThreadContext threadContext;
    private final Client client;
    final AsyncSearchSecurity security;
    private final Client clientWithOrigin;
    private final NamedWriteableRegistry registry;
    private final Writeable.Reader<R> reader;
    private final BigArrays bigArrays;
    private volatile long maxResponseSize;
    private final ClusterService clusterService;
    private final CircuitBreaker circuitBreaker;

    public AsyncTaskIndexService(
        String index,
        ClusterService clusterService,
        ThreadContext threadContext,
        Client client,
        String origin,
        Writeable.Reader<R> reader,
        NamedWriteableRegistry registry,
        BigArrays bigArrays
    ) {
        this.index = index;
        this.threadContext = threadContext;
        this.client = client;
        this.security = new AsyncSearchSecurity(
            index,
            new SecurityContext(clusterService.getSettings(), client.threadPool().getThreadContext()),
            client,
            origin
        );
        this.clientWithOrigin = new OriginSettingClient(client, origin);
        this.registry = registry;
        this.reader = reader;
        this.bigArrays = bigArrays;
        this.maxResponseSize = MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING.get(clusterService.getSettings()).getBytes();
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING, (v) -> maxResponseSize = v.getBytes());
        this.clusterService = clusterService;
        this.circuitBreaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
    }

    /**
     * Returns the internal client wrapped with the async user origin.
     */
    public Client getClientWithOrigin() {
        return clientWithOrigin;
    }

    /**
     * Returns the internal client.
     */
    public Client getClient() {
        return client;
    }

    public AsyncSearchSecurity getSecurity() {
        return security;
    }

    /**
     * Stores the initial response with the original headers of the authenticated user
     * and the expected expiration time.
     * Currently for EQL we don't set limit for a stored async response
     * TODO: add limit for stored async response in EQL, and instead of this method use createResponse
     */
    public void createResponseForEQL(String docId, Map<String, String> headers, R response, ActionListener<DocWriteResponse> listener) {
        indexResponse(docId, headers, null, response, false, listener);
    }

    public void createResponseForEQL(
        String docId,
        Map<String, String> headers,
        Map<String, List<String>> responseHeaders,
        R response,
        ActionListener<DocWriteResponse> listener
    ) {
        indexResponse(docId, headers, responseHeaders, response, false, listener);
    }

    /**
     * Stores the initial response with the original headers of the authenticated user
     * and the expected expiration time.
     */
    public void createResponse(String docId, Map<String, String> headers, R response, ActionListener<DocWriteResponse> listener) {
        indexResponse(docId, headers, null, response, true, listener);
    }

    public void updateResponse(
        String docId,
        Map<String, List<String>> responseHeaders,
        R response,
        ActionListener<UpdateResponse> listener
    ) {
        updateResponse(docId, responseHeaders, response, listener, false);
    }

    private void indexResponse(
        String docId,
        Map<String, String> headers,
        @Nullable Map<String, List<String>> responseHeaders,
        R response,
        boolean limitToMaxResponseSize,
        ActionListener<DocWriteResponse> listener
    ) {
        try {
            var buffer = allocateBuffer(limitToMaxResponseSize);
            listener = ActionListener.runBefore(listener, buffer::close);
            final XContentBuilder source = jsonBuilder(buffer).startObject()
                .field(HEADERS_FIELD, headers)
                .field(EXPIRATION_TIME_FIELD, response.getExpirationTime());
            if (responseHeaders != null) {
                source.field(RESPONSE_HEADERS_FIELD, responseHeaders);
            }

            addResultFieldAndFinish(response, source);
            clientWithOrigin.index(new IndexRequest(index).create(true).id(docId).source(buffer.bytes(), source.contentType()), listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Stores the final response if the place-holder document is still present (update).
     */
    private void updateResponse(
        String docId,
        Map<String, List<String>> responseHeaders,
        R response,
        ActionListener<UpdateResponse> listener,
        boolean isFailure
    ) {
        ReleasableBytesStreamOutput buffer = null;
        try {
            buffer = allocateBuffer(isFailure == false);
            final XContentBuilder source = jsonBuilder(buffer).startObject().field(RESPONSE_HEADERS_FIELD, responseHeaders);
            addResultFieldAndFinish(response, source);
            clientWithOrigin.update(
                new UpdateRequest().index(index).id(docId).doc(buffer.bytes(), source.contentType()).retryOnConflict(5),
                ActionListener.runBefore(listener, buffer::close)
            );
        } catch (Exception e) {
            // release buffer right away to save memory, particularly in case the exception came from the circuit breaker
            Releasables.close(buffer);
            // even if we expect updating with a failure always succeed
            // this is just an extra precaution not to create infinite loops
            if (isFailure) {
                listener.onFailure(e);
            } else {
                Throwable cause = ExceptionsHelper.unwrapCause(e);
                if (cause instanceof DocumentMissingException == false && cause instanceof VersionConflictEngineException == false) {
                    logger.error(() -> "failed to store async-search [" + docId + "]", e);
                    // at end, we should report a failure to the listener
                    updateResponse(
                        docId,
                        responseHeaders,
                        response.convertToFailure(e),
                        ActionListener.running(() -> listener.onFailure(e)),
                        true
                    );
                } else {
                    listener.onFailure(e);
                }
            }
        }
    }

    private ReleasableBytesStreamOutput allocateBuffer(boolean limitToMaxResponseSize) {
        return limitToMaxResponseSize
            ? new ReleasableBytesStreamOutputWithLimit(0, bigArrays.withCircuitBreaking(), maxResponseSize)
            : new ReleasableBytesStreamOutput(0, bigArrays.withCircuitBreaking());
    }

    private void addResultFieldAndFinish(Writeable response, XContentBuilder source) throws IOException {
        source.directFieldAsBase64(RESULT_FIELD, os -> {
            // do not close the output
            os = Streams.noCloseStream(os);
            TransportVersion minNodeVersion = clusterService.state().getMinTransportVersion();
            TransportVersion.writeVersion(minNodeVersion, new OutputStreamStreamOutput(os));
            os = CompressorFactory.COMPRESSOR.threadLocalOutputStream(os);
            try (OutputStreamStreamOutput out = new OutputStreamStreamOutput(os)) {
                out.setTransportVersion(minNodeVersion);
                response.writeTo(out);
            }
        }).endObject();
        // do not close the buffer or the XContentBuilder until the request is completed (i.e., listener is notified);
        // otherwise, we underestimate the memory usage in case the circuit breaker does not use the real memory usage.
        source.flush();
    }

    /**
     * Updates the expiration time of the provided <code>docId</code> if the place-holder
     * document is still present (update).
     */
    public void updateExpirationTime(String docId, long expirationTimeMillis, ActionListener<UpdateResponse> listener) {
        Map<String, Object> source = Collections.singletonMap(EXPIRATION_TIME_FIELD, expirationTimeMillis);
        UpdateRequest request = new UpdateRequest().index(index).id(docId).doc(source, XContentType.JSON).retryOnConflict(5);
        clientWithOrigin.update(request, listener);
    }

    /**
     * Deletes the provided <code>asyncTaskId</code> from the index if present.
     */
    public void deleteResponse(AsyncExecutionId asyncExecutionId, ActionListener<DeleteResponse> listener) {
        try {
            DeleteRequest request = new DeleteRequest(index).id(asyncExecutionId.getDocId());
            clientWithOrigin.delete(request, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Returns the {@link AsyncTask} if the provided <code>asyncTaskId</code>
     * is registered in the task manager, <code>null</code> otherwise.
     */
    public static <T extends AsyncTask> T getTask(TaskManager taskManager, AsyncExecutionId asyncExecutionId, Class<T> tClass)
        throws IOException {
        Task task = taskManager.getTask(asyncExecutionId.getTaskId().getId());
        if (tClass.isInstance(task) == false) {
            return null;
        }
        @SuppressWarnings("unchecked")
        T asyncTask = (T) task;
        if (asyncTask.getExecutionId().equals(asyncExecutionId) == false) {
            return null;
        }
        return asyncTask;
    }

    /**
     * Returns the {@link AsyncTask} if the provided <code>asyncTaskId</code>
     * is registered in the task manager, <code>null</code> otherwise.
     *
     * This method throws a {@link ResourceNotFoundException} if the authenticated user
     * is not the creator of the original task.
     */
    public <T extends AsyncTask> T getTaskAndCheckAuthentication(
        TaskManager taskManager,
        AsyncExecutionId asyncExecutionId,
        Class<T> tClass
    ) throws IOException {
        return getTaskAndCheckAuthentication(taskManager, security, asyncExecutionId, tClass);
    }

    /**
    * Returns the {@link AsyncTask} if the provided <code>asyncTaskId</code>
    * is registered in the task manager, <code>null</code> otherwise.
    *
    * This method throws a {@link ResourceNotFoundException} if the authenticated user
    * is not the creator of the original task.
    */
    public static <T extends AsyncTask> T getTaskAndCheckAuthentication(
        TaskManager taskManager,
        AsyncSearchSecurity security,
        AsyncExecutionId asyncExecutionId,
        Class<T> tClass
    ) throws IOException {
        T asyncTask = getTask(taskManager, asyncExecutionId, tClass);
        if (asyncTask == null) {
            return null;
        }
        // Check authentication for the user
        if (false == security.currentUserHasAccessToTask(asyncTask)) {
            throw new ResourceNotFoundException(asyncExecutionId.getEncoded() + " not found");
        }
        return asyncTask;
    }

    /**
     * Gets the response from the index if present, or delegate a {@link ResourceNotFoundException}
     * failure to the provided listener if not.
     * When the provided <code>restoreResponseHeaders</code> is <code>true</code>, this method also restores the
     * response headers of the original request in the current thread context.
     */
    public void getResponse(AsyncExecutionId asyncExecutionId, boolean restoreResponseHeaders, ActionListener<R> listener) {
        getResponseFromIndex(asyncExecutionId, restoreResponseHeaders, true, listener);
    }

    private void getResponseFromIndex(
        AsyncExecutionId asyncExecutionId,
        boolean restoreResponseHeaders,
        boolean checkAuthentication,
        ActionListener<R> outerListener
    ) {
        final GetRequest getRequest = new GetRequest(index).preference(asyncExecutionId.getEncoded())
            .id(asyncExecutionId.getDocId())
            .realtime(true);
        clientWithOrigin.get(getRequest, outerListener.delegateFailure((listener, getResponse) -> {
            if (getResponse.isExists() == false) {
                listener.onFailure(new ResourceNotFoundException(asyncExecutionId.getEncoded()));
                return;
            }
            final R resp;
            try {
                final BytesReference source = getResponse.getSourceInternal();
                // reserve twice memory of the source length: one for the internal XContent parser and one for the response
                final long reservedBytes = source.length() * 2L;
                circuitBreaker.addEstimateBytesAndMaybeBreak(reservedBytes, "decode async response");
                listener = ActionListener.runAfter(listener, () -> circuitBreaker.addWithoutBreaking(-reservedBytes));
                resp = parseResponseFromIndex(asyncExecutionId, source, restoreResponseHeaders, checkAuthentication);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            ActionListener.respondAndRelease(listener, resp);
        }));
    }

    private R parseResponseFromIndex(
        AsyncExecutionId asyncExecutionId,
        BytesReference source,
        boolean restoreResponseHeaders,
        boolean checkAuthentication
    ) {
        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                source,
                XContentType.JSON
            )
        ) {
            ensureExpectedToken(parser.nextToken(), XContentParser.Token.START_OBJECT, parser);
            R resp = null;
            Long expirationTime = null;
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
                parser.nextToken();
                switch (parser.currentName()) {
                    case RESULT_FIELD -> resp = decodeResponse(parser.charBuffer());
                    case EXPIRATION_TIME_FIELD -> expirationTime = (long) parser.numberValue();
                    case HEADERS_FIELD -> {
                        @SuppressWarnings("unchecked")
                        final Map<String, String> headers = (Map<String, String>) XContentParserUtils.parseFieldsValue(parser);
                        // check the authentication of the current user against the user that initiated the async task
                        if (checkAuthentication && false == security.currentUserHasAccessToTaskWithHeaders(headers)) {
                            throw new ResourceNotFoundException(asyncExecutionId.getEncoded());
                        }
                    }
                    case RESPONSE_HEADERS_FIELD -> {
                        @SuppressWarnings("unchecked")
                        final Map<String, List<String>> responseHeaders = (Map<String, List<String>>) XContentParserUtils.parseFieldsValue(
                            parser
                        );
                        if (restoreResponseHeaders) {
                            restoreResponseHeadersContext(threadContext, responseHeaders);
                        }
                    }
                    default -> XContentParserUtils.parseFieldsValue(parser); // consume and discard unknown fields
                }
            }
            Objects.requireNonNull(resp, "Get result doesn't include [" + RESULT_FIELD + "] field");
            Objects.requireNonNull(expirationTime, "Get result doesn't include [" + EXPIRATION_TIME_FIELD + "] field");
            try {
                return resp.withExpirationTime(expirationTime);
            } finally {
                resp.decRef();
            }
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse the get result", e);
        }
    }

    /**
     * Retrieve the status of the async search or async or stored eql search.
     * Retrieve from the task if the task is still available or from the index.
     */
    public <T extends AsyncTask, SR extends SearchStatusResponse> void retrieveStatus(
        GetAsyncStatusRequest request,
        TaskManager taskManager,
        Class<T> tClass,
        Function<T, SR> statusProducerFromTask,
        TriFunction<R, Long, String, SR> statusProducerFromIndex,
        ActionListener<SR> originalListener
    ) {
        // check if the result has expired
        final ActionListener<SR> outerListener = originalListener.delegateFailure((listener, resp) -> {
            if (resp.getExpirationTime() < System.currentTimeMillis()) {
                listener.onFailure(new ResourceNotFoundException(request.getId()));
            } else {
                listener.onResponse(resp);
            }
        });
        security.currentUserCanSeeStatusOfAllSearches(ActionListener.wrap(canSeeAll -> {
            AsyncExecutionId asyncExecutionId = AsyncExecutionId.decode(request.getId());
            try {
                T asyncTask = getTask(taskManager, asyncExecutionId, tClass);
                if (asyncTask != null) { // get status response from task
                    if (canSeeAll || security.currentUserHasAccessToTask(asyncTask)) {
                        var response = statusProducerFromTask.apply(asyncTask);
                        outerListener.onResponse(response);
                    } else {
                        outerListener.onFailure(new ResourceNotFoundException(request.getId()));
                    }
                } else {
                    // get status response from index
                    final boolean checkAuthentication = canSeeAll == false;
                    getResponseFromIndex(
                        asyncExecutionId,
                        false,
                        checkAuthentication,
                        outerListener.map(
                            resp -> statusProducerFromIndex.apply(resp, resp.getExpirationTime(), asyncExecutionId.getEncoded())
                        )
                    );
                }
            } catch (Exception exc) {
                outerListener.onFailure(exc);
            }
        }, outerListener::onFailure));
    }

    /**
     * Decode the provided base-64 bytes into a {@link AsyncSearchResponse}.
     */
    private R decodeResponse(CharBuffer encodedBuffer) throws IOException {
        InputStream encodedIn = Base64.getDecoder().wrap(new InputStream() {
            @Override
            public int read() {
                if (encodedBuffer.hasRemaining()) {
                    return encodedBuffer.get();
                } else {
                    return -1; // end of stream
                }
            }
        });
        TransportVersion version = TransportVersion.readVersion(new InputStreamStreamInput(encodedIn));
        assert version.onOrBefore(TransportVersion.current()) : version + " >= " + TransportVersion.current();
        final StreamInput input;
        input = CompressorFactory.COMPRESSOR.threadLocalStreamInput(encodedIn);
        try (StreamInput in = new NamedWriteableAwareStreamInput(input, registry)) {
            in.setTransportVersion(version);
            return reader.read(in);
        }
    }

    /**
     * Restores the provided <code>responseHeaders</code> to the current thread context.
     */
    public static void restoreResponseHeadersContext(ThreadContext threadContext, Map<String, List<String>> responseHeaders) {
        for (Map.Entry<String, List<String>> entry : responseHeaders.entrySet()) {
            for (String value : entry.getValue()) {
                threadContext.addResponseHeader(entry.getKey(), value);
            }
        }
    }

    private static class ReleasableBytesStreamOutputWithLimit extends ReleasableBytesStreamOutput {
        private final long limit;

        ReleasableBytesStreamOutputWithLimit(int expectedSize, BigArrays bigArrays, long limit) {
            super(expectedSize, bigArrays);
            this.limit = limit;
        }

        @Override
        protected void ensureCapacity(long offset) {
            if (offset > limit) {
                throw new IllegalArgumentException(
                    "Can't store an async search response larger than ["
                        + limit
                        + "] bytes. "
                        + "This limit can be set by changing the ["
                        + MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING.getKey()
                        + "] setting."
                );
            }
            super.ensureCapacity(offset);
        }
    }
}
