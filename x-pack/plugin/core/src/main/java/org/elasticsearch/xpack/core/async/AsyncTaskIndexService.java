/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
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
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.SearchStatusResponse;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.CharBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.search.SearchService.MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING;
import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.AUTHENTICATION_KEY;

/**
 * A service that exposes the CRUD operations for the async task-specific index.
 */
public final class AsyncTaskIndexService<R extends AsyncResponse<R>> {
    private static final Logger logger = LogManager.getLogger(AsyncTaskIndexService.class);

    public static final String HEADERS_FIELD = "headers";
    public static final String RESPONSE_HEADERS_FIELD = "response_headers";
    public static final String EXPIRATION_TIME_FIELD = "expiration_time";
    public static final String RESULT_FIELD = "result";

    // Usually the settings, mappings and system index descriptor below
    // would be co-located with the SystemIndexPlugin implementation,
    // however in this case this service is in a different project to
    // AsyncResultsIndexPlugin, as are tests that need access to
    // #settings().

    static Settings settings() {
        return Settings.builder()
            .put("index.codec", "best_compression")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .build();
    }

    private static XContentBuilder mappings() {
        try {
            XContentBuilder builder = jsonBuilder()
                .startObject()
                    .startObject(SINGLE_MAPPING_NAME)
                        .startObject("_meta")
                            .field("version", Version.CURRENT)
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
            .setVersionMetaKey("version")
            .setOrigin(ASYNC_SEARCH_ORIGIN)
            .build();
    }

    private final String index;
    private final Client client;
    private final Client clientWithOrigin;
    private final SecurityContext securityContext;
    private final NamedWriteableRegistry registry;
    private final Writeable.Reader<R> reader;
    private final BigArrays bigArrays;
    private volatile long maxResponseSize;
    private final ClusterService clusterService;
    private final CircuitBreaker circuitBreaker;

    public AsyncTaskIndexService(String index,
                                 ClusterService clusterService,
                                 ThreadContext threadContext,
                                 Client client,
                                 String origin,
                                 Writeable.Reader<R> reader,
                                 NamedWriteableRegistry registry,
                                 BigArrays bigArrays) {
        this.index = index;
        this.securityContext = new SecurityContext(clusterService.getSettings(), threadContext);
        this.client = client;
        this.clientWithOrigin = new OriginSettingClient(client, origin);
        this.registry = registry;
        this.reader = reader;
        this.bigArrays = bigArrays;
        this.maxResponseSize = MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING.get(clusterService.getSettings()).getBytes();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(
            MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING, (v) -> maxResponseSize = v.getBytes());
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

    /**
     * Returns the authentication information, or null if the current context has no authentication info.
     **/
    public Authentication getAuthentication() {
        return securityContext.getAuthentication();
    }

    /**
     * Stores the initial response with the original headers of the authenticated user
     * and the expected expiration time.
     * Currently for EQL we don't set limit for a stored async response
     * TODO: add limit for stored async response in EQL, and instead of this method use createResponse
     */
    public void createResponseForEQL(String docId,
                               Map<String, String> headers,
                               R response,
                               ActionListener<IndexResponse> listener) throws IOException {
        try {
            final ReleasableBytesStreamOutput buffer = new ReleasableBytesStreamOutput(0, bigArrays.withCircuitBreaking());
            final XContentBuilder source = XContentFactory.jsonBuilder(buffer);
            listener = ActionListener.runBefore(listener, buffer::close);
            source
                .startObject()
                .field(HEADERS_FIELD, headers)
                .field(EXPIRATION_TIME_FIELD, response.getExpirationTime())
                .directFieldAsBase64(RESULT_FIELD, os -> writeResponse(response, os))
                .endObject();

            // do not close the buffer or the XContentBuilder until the IndexRequest is completed (i.e., listener is notified);
            // otherwise, we underestimate the memory usage in case the circuit breaker does not use the real memory usage.
            source.flush();
            final IndexRequest indexRequest = new IndexRequest(index)
                .create(true)
                .id(docId)
                .source(buffer.bytes(), source.contentType());
            clientWithOrigin.index(indexRequest, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Stores the initial response with the original headers of the authenticated user
     * and the expected expiration time.
     */
    public void createResponse(String docId,
                               Map<String, String> headers,
                               R response,
                               ActionListener<IndexResponse> listener) throws IOException {
        try {
            final ReleasableBytesStreamOutput buffer = new ReleasableBytesStreamOutputWithLimit(
                0, bigArrays.withCircuitBreaking(), maxResponseSize);
            final XContentBuilder source = XContentFactory.jsonBuilder(buffer);
            listener = ActionListener.runBefore(listener, buffer::close);
            source
                .startObject()
                .field(HEADERS_FIELD, headers)
                .field(EXPIRATION_TIME_FIELD, response.getExpirationTime())
                .directFieldAsBase64(RESULT_FIELD, os -> writeResponse(response, os))
                .endObject();

            // do not close the buffer or the XContentBuilder until the IndexRequest is completed (i.e., listener is notified);
            // otherwise, we underestimate the memory usage in case the circuit breaker does not use the real memory usage.
            source.flush();
            final IndexRequest indexRequest = new IndexRequest(index)
                .create(true)
                .id(docId)
                .source(buffer.bytes(), source.contentType());
            clientWithOrigin.index(indexRequest, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public void updateResponse(String docId,
                               Map<String, List<String>> responseHeaders,
                               R response,
                               ActionListener<UpdateResponse> listener) {
        updateResponse(docId, responseHeaders, response, listener, false);
    }

    /**
     * Stores the final response if the place-holder document is still present (update).
     */
    private void updateResponse(String docId,
                                Map<String, List<String>> responseHeaders,
                                R response,
                                ActionListener<UpdateResponse> listener,
                                boolean isFailure) {
        try {
            final ReleasableBytesStreamOutput buffer = isFailure ?
                new ReleasableBytesStreamOutput(0, bigArrays.withCircuitBreaking()) :
                new ReleasableBytesStreamOutputWithLimit(0, bigArrays.withCircuitBreaking(), maxResponseSize);
            final XContentBuilder source = XContentFactory.jsonBuilder(buffer);
            listener = ActionListener.runBefore(listener, buffer::close);
            source
                .startObject()
                .field(RESPONSE_HEADERS_FIELD, responseHeaders)
                .directFieldAsBase64(RESULT_FIELD, os -> writeResponse(response, os))
                .endObject();
            // do not close the buffer or the XContentBuilder until the UpdateRequest is completed (i.e., listener is notified);
            // otherwise, we underestimate the memory usage in case the circuit breaker does not use the real memory usage.
            source.flush();
            final UpdateRequest request = new UpdateRequest()
                .index(index)
                .id(docId)
                .doc(buffer.bytes(), source.contentType())
                .retryOnConflict(5);
            clientWithOrigin.update(request, listener);
        } catch (Exception e) {
            // even if we expect updating with a failure always succeed
            // this is just an extra precaution not to create infinite loops
            if (isFailure) {
                listener.onFailure(e);
            } else {
                Throwable cause = ExceptionsHelper.unwrapCause(e);
                if (cause instanceof DocumentMissingException == false && cause instanceof VersionConflictEngineException == false) {
                    logger.error(() -> new ParameterizedMessage("failed to store async-search [{}]", docId), e);
                    ActionListener<UpdateResponse> newListener = listener;
                    updateStoredResponseWithFailure(
                        docId,
                        responseHeaders,
                        response,
                        e,
                        // at end, we should report a failure to the listener
                        ActionListener.wrap(() -> newListener.onFailure(e))
                    );
                } else {
                    listener.onFailure(e);
                }
            }
        }
    }

    /**
     * Update the initial stored response with a failure
     */
    private void updateStoredResponseWithFailure(String docId,
                                                 Map<String, List<String>> responseHeaders,
                                                 R response,
                                                 Exception updateException,
                                                 ActionListener<UpdateResponse> listener) {
        R failureResponse = response.convertToFailure(updateException);
        updateResponse(docId, responseHeaders, failureResponse, listener, true);
    }

    /**
     * Updates the expiration time of the provided <code>docId</code> if the place-holder
     * document is still present (update).
     */
    public void updateExpirationTime(String docId,
                              long expirationTimeMillis,
                              ActionListener<UpdateResponse> listener) {
        Map<String, Object> source = Collections.singletonMap(EXPIRATION_TIME_FIELD, expirationTimeMillis);
        UpdateRequest request = new UpdateRequest().index(index)
            .id(docId)
            .doc(source, XContentType.JSON)
            .retryOnConflict(5);
        clientWithOrigin.update(request, listener);
    }

    /**
     * Deletes the provided <code>asyncTaskId</code> from the index if present.
     */
    public void deleteResponse(AsyncExecutionId asyncExecutionId,
                               ActionListener<DeleteResponse> listener) {
        try {
            DeleteRequest request = new DeleteRequest(index).id(asyncExecutionId.getDocId());
            clientWithOrigin.delete(request, listener);
        } catch(Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Returns the {@link AsyncTask} if the provided <code>asyncTaskId</code>
     * is registered in the task manager, <code>null</code> otherwise.
     */
    public <T extends AsyncTask> T getTask(TaskManager taskManager,
                                           AsyncExecutionId asyncExecutionId,
                                           Class<T> tClass) throws IOException {
        Task task = taskManager.getTask(asyncExecutionId.getTaskId().getId());
        if (tClass.isInstance(task) == false) {
            return null;
        }
        @SuppressWarnings("unchecked") T asyncTask = (T) task;
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
    public <T extends AsyncTask> T getTaskAndCheckAuthentication(TaskManager taskManager,
                                                                 AsyncExecutionId asyncExecutionId,
                                                                 Class<T> tClass) throws IOException {
        T asyncTask = getTask(taskManager, asyncExecutionId, tClass);
        if (asyncTask == null) {
            return null;
        }
        // Check authentication for the user
        final Authentication auth = securityContext.getAuthentication();
        if (ensureAuthenticatedUserIsSame(asyncTask.getOriginHeaders(), auth) == false) {
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
    public void getResponse(AsyncExecutionId asyncExecutionId,
                            boolean restoreResponseHeaders,
                            ActionListener<R> listener) {
        getResponseFromIndex(asyncExecutionId, restoreResponseHeaders, true, listener);
    }

    private void getResponseFromIndex(AsyncExecutionId asyncExecutionId,
                                      boolean restoreResponseHeaders,
                                      boolean checkAuthentication,
                                      ActionListener<R> outerListener) {
        final GetRequest getRequest = new GetRequest(index)
            .preference(asyncExecutionId.getEncoded())
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
                final int reservedBytes = source.length() * 2;
                circuitBreaker.addEstimateBytesAndMaybeBreak(source.length() * 2L, "decode async response");
                listener = ActionListener.runAfter(listener, () -> circuitBreaker.addWithoutBreaking(-reservedBytes));
                resp = parseResponseFromIndex(asyncExecutionId, source, restoreResponseHeaders, checkAuthentication);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            listener.onResponse(resp);
        }));
    }

    private R parseResponseFromIndex(AsyncExecutionId asyncExecutionId, BytesReference source,
                                     boolean restoreResponseHeaders, boolean checkAuthentication) {
        try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, source, XContentType.JSON)) {
            ensureExpectedToken(parser.nextToken(), XContentParser.Token.START_OBJECT, parser);
            R resp = null;
            Long expirationTime = null;
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
                parser.nextToken();
                switch (parser.currentName()) {
                    case RESULT_FIELD:
                        resp = decodeResponse(parser.charBuffer());
                        break;
                    case EXPIRATION_TIME_FIELD:
                        expirationTime = (long) parser.numberValue();
                        break;
                    case HEADERS_FIELD:
                        @SuppressWarnings("unchecked") final Map<String, String> headers =
                            (Map<String, String>) XContentParserUtils.parseFieldsValue(parser);
                        // check the authentication of the current user against the user that initiated the async task
                        if (checkAuthentication &&
                            ensureAuthenticatedUserIsSame(headers, securityContext.getAuthentication()) == false) {
                            throw new ResourceNotFoundException(asyncExecutionId.getEncoded());
                        }
                        break;
                    case RESPONSE_HEADERS_FIELD:
                        @SuppressWarnings("unchecked") final Map<String, List<String>> responseHeaders =
                            (Map<String, List<String>>) XContentParserUtils.parseFieldsValue(parser);
                        if (restoreResponseHeaders) {
                            restoreResponseHeadersContext(securityContext.getThreadContext(), responseHeaders);
                        }
                        break;
                    default:
                        XContentParserUtils.parseFieldsValue(parser); // consume and discard unknown fields
                        break;
                }
            }
            Objects.requireNonNull(resp, "Get result doesn't include [" + RESULT_FIELD + "] field");
            Objects.requireNonNull(expirationTime, "Get result doesn't include [" + EXPIRATION_TIME_FIELD + "] field");
            return resp.withExpirationTime(expirationTime);
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
        ActionListener<SR> outerListener) {
        // check if the result has expired
        outerListener = outerListener.delegateFailure((listener, resp) -> {
            if (resp.getExpirationTime() < System.currentTimeMillis()) {
                listener.onFailure(new ResourceNotFoundException(request.getId()));
            } else {
                listener.onResponse(resp);
            }
        });
        AsyncExecutionId asyncExecutionId = AsyncExecutionId.decode(request.getId());
        try {
            T asyncTask = getTask(taskManager, asyncExecutionId, tClass);
            if (asyncTask != null) { // get status response from task
                SR response = statusProducerFromTask.apply(asyncTask);
                outerListener.onResponse(response);
            } else {
                // get status response from index
                getResponseFromIndex(asyncExecutionId, false, false, outerListener.delegateFailure((listener, resp) ->
                    listener.onResponse(statusProducerFromIndex.apply(resp, resp.getExpirationTime(), asyncExecutionId.getEncoded()))));
            }
        } catch (Exception exc) {
            outerListener.onFailure(exc);
        }
    }

    /**
     * Checks if the current user's authentication matches the original authentication stored
     * in the async search index.
     **/
    void ensureAuthenticatedUserCanDeleteFromIndex(AsyncExecutionId executionId, ActionListener<Void> listener) {
        GetRequest internalGet = new GetRequest(index)
            .preference(executionId.getEncoded())
            .id(executionId.getDocId())
            .fetchSourceContext(new FetchSourceContext(true, new String[] { HEADERS_FIELD }, new String[] {}));

        clientWithOrigin.get(internalGet, ActionListener.wrap(
            get -> {
                if (get.isExists() == false) {
                    listener.onFailure(new ResourceNotFoundException(executionId.getEncoded()));
                    return;
                }
                // Check authentication for the user
                @SuppressWarnings("unchecked")
                Map<String, String> headers = (Map<String, String>) get.getSource().get(HEADERS_FIELD);
                if (ensureAuthenticatedUserIsSame(headers, securityContext.getAuthentication())) {
                    listener.onResponse(null);
                } else {
                    listener.onFailure(new ResourceNotFoundException(executionId.getEncoded()));
                }
            },
            exc -> listener.onFailure(new ResourceNotFoundException(executionId.getEncoded()))));
    }

    /**
     * Extracts the authentication from the original headers and checks that it matches
     * the current user. This function returns always <code>true</code> if the provided
     * <code>headers</code> do not contain any authentication.
     */
    boolean ensureAuthenticatedUserIsSame(Map<String, String> originHeaders, Authentication current) throws IOException {
        if (originHeaders == null || originHeaders.containsKey(AUTHENTICATION_KEY) == false) {
            // no authorization attached to the original request
            return true;
        }
        if (current == null) {
            // origin is an authenticated user but current is not
            return false;
        }
        Authentication origin = AuthenticationContextSerializer.decode(originHeaders.get(AUTHENTICATION_KEY));
        return origin.canAccessResourcesOf(current);
    }

    private void writeResponse(R response, OutputStream os) throws IOException {
        os = new FilterOutputStream(os) {
            @Override
            public void close() {
                // do not close the output
            }
        };
        final Version minNodeVersion = clusterService.state().nodes().getMinNodeVersion();
        Version.writeVersion(minNodeVersion, new OutputStreamStreamOutput(os));
        if (minNodeVersion.onOrAfter(Version.V_7_15_0)) {
            os = CompressorFactory.COMPRESSOR.threadLocalOutputStream(os);
        }
        try (OutputStreamStreamOutput out = new OutputStreamStreamOutput(os)) {
            out.setVersion(minNodeVersion);
            response.writeTo(out);
        }
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
        final Version version = Version.readVersion(new InputStreamStreamInput(encodedIn));
        assert version.onOrBefore(Version.CURRENT) : version + " >= " + Version.CURRENT;
        if (version.onOrAfter(Version.V_7_15_0)) {
            encodedIn = CompressorFactory.COMPRESSOR.threadLocalInputStream(encodedIn);
        }
        try (StreamInput in = new NamedWriteableAwareStreamInput(new InputStreamStreamInput(encodedIn), registry)) {
            in.setVersion(version);
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
                throw new IllegalArgumentException("Can't store an async search response larger than [" + limit + "] bytes. " +
                    "This limit can be set by changing the [" + MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING.getKey() + "] setting.");
            }
            super.ensureCapacity(offset);
        }
    }
}
