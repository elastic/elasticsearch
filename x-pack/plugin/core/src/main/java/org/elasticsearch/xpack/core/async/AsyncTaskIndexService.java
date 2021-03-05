/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.async;

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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.AUTHENTICATION_KEY;

/**
 * A service that exposes the CRUD operations for the async task-specific index.
 */
public final class AsyncTaskIndexService<R extends AsyncResponse<R>> {

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
            .setIndexPattern(XPackPlugin.ASYNC_RESULTS_INDEX)
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

    public AsyncTaskIndexService(String index,
                                 ClusterService clusterService,
                                 ThreadContext threadContext,
                                 Client client,
                                 String origin,
                                 Writeable.Reader<R> reader,
                                 NamedWriteableRegistry registry) {
        this.index = index;
        this.securityContext = new SecurityContext(clusterService.getSettings(), threadContext);
        this.client = client;
        this.clientWithOrigin = new OriginSettingClient(client, origin);
        this.registry = registry;
        this.reader = reader;
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
     */
    public void createResponse(String docId,
                               Map<String, String> headers,
                               R response,
                               ActionListener<IndexResponse> listener) throws IOException {
        Map<String, Object> source = new HashMap<>();
        source.put(HEADERS_FIELD, headers);
        source.put(EXPIRATION_TIME_FIELD, response.getExpirationTime());
        source.put(RESULT_FIELD, encodeResponse(response));
        IndexRequest indexRequest = new IndexRequest(index)
            .create(true)
            .id(docId)
            .source(source, XContentType.JSON);
        clientWithOrigin.index(indexRequest, listener);
    }

    /**
     * Stores the final response if the place-holder document is still present (update).
     */
    public void updateResponse(String docId,
                            Map<String, List<String>> responseHeaders,
                            R response,
                            ActionListener<UpdateResponse> listener) {
        try {
            Map<String, Object> source = new HashMap<>();
            source.put(RESPONSE_HEADERS_FIELD, responseHeaders);
            source.put(RESULT_FIELD, encodeResponse(response));
            UpdateRequest request = new UpdateRequest()
                .index(index)
                .id(docId)
                .doc(source, XContentType.JSON)
                .retryOnConflict(5);
            clientWithOrigin.update(request, listener);
        } catch(Exception e) {
            listener.onFailure(e);
        }
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

    private void getEncodedResponse(AsyncExecutionId asyncExecutionId,
                                    boolean restoreResponseHeaders,
                                    ActionListener<Tuple<String, Long>> listener) {
        GetRequest internalGet = new GetRequest(index)
            .preference(asyncExecutionId.getEncoded())
            .id(asyncExecutionId.getDocId());
        clientWithOrigin.get(internalGet, ActionListener.wrap(
            get -> {
                if (get.isExists() == false) {
                    listener.onFailure(new ResourceNotFoundException(asyncExecutionId.getEncoded()));
                    return;
                }

                // check the authentication of the current user against the user that initiated the async task
                @SuppressWarnings("unchecked")
                Map<String, String> headers = (Map<String, String>) get.getSource().get(HEADERS_FIELD);
                if (ensureAuthenticatedUserIsSame(headers, securityContext.getAuthentication()) == false) {
                    listener.onFailure(new ResourceNotFoundException(asyncExecutionId.getEncoded()));
                    return;
                }

                if (restoreResponseHeaders && get.getSource().containsKey(RESPONSE_HEADERS_FIELD)) {
                    @SuppressWarnings("unchecked")
                    Map<String, List<String>> responseHeaders = (Map<String, List<String>>) get.getSource().get(RESPONSE_HEADERS_FIELD);
                    restoreResponseHeadersContext(securityContext.getThreadContext(), responseHeaders);
                }

                long expirationTime = (long) get.getSource().get(EXPIRATION_TIME_FIELD);
                String encoded = (String) get.getSource().get(RESULT_FIELD);
                if (encoded != null) {
                    listener.onResponse(new Tuple<>(encoded, expirationTime));
                } else {
                    listener.onResponse(null);
                }
            },
            listener::onFailure
        ));
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
        getEncodedResponse(asyncExecutionId, restoreResponseHeaders, ActionListener.wrap(
            (t) -> listener.onResponse(decodeResponse(t.v1()).withExpirationTime(t.v2())),
            listener::onFailure
        ));
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
            ActionListener<SR> listener) {
        AsyncExecutionId asyncExecutionId = AsyncExecutionId.decode(request.getId());
        try {
            T asyncTask = getTask(taskManager, asyncExecutionId, tClass);
            if (asyncTask != null) { // get status response from task
                SR response = statusProducerFromTask.apply(asyncTask);
                sendFinalStatusResponse(request, response, listener);
            } else { // get status response from index
                getStatusResponseFromIndex(asyncExecutionId, statusProducerFromIndex,
                    new ActionListener<>() {
                        @Override
                        public void onResponse(SR searchStatusResponse) {
                            sendFinalStatusResponse(request, searchStatusResponse, listener);
                        }
                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }
                    }
                );
            }
        } catch (Exception exc) {
            listener.onFailure(exc);
        }
    }

    /**
     * Gets the status response of the stored search from the index
     * @param asyncExecutionId – id of the stored search (async search or stored eql search)
     * @param statusProducer – a producer of a status from the stored search, expirationTime and async search id
     * @param listener – listener to report result to
     */
    private <SR extends SearchStatusResponse> void getStatusResponseFromIndex(
        AsyncExecutionId asyncExecutionId,
        TriFunction<R, Long, String, SR> statusProducer,
        ActionListener<SR> listener) {
        String asyncId = asyncExecutionId.getEncoded();
        GetRequest internalGet = new GetRequest(index)
            .preference(asyncId)
            .id(asyncExecutionId.getDocId());
        clientWithOrigin.get(internalGet, ActionListener.wrap(
            get -> {
                if (get.isExists() == false) {
                    listener.onFailure(new ResourceNotFoundException(asyncExecutionId.getEncoded()));
                    return;
                }
                String encoded = (String) get.getSource().get(RESULT_FIELD);
                if (encoded != null) {
                    Long expirationTime = (Long) get.getSource().get(EXPIRATION_TIME_FIELD);
                    listener.onResponse(statusProducer.apply(decodeResponse(encoded), expirationTime, asyncId));
                } else {
                    listener.onResponse(null);
                }
            },
            listener::onFailure
        ));
    }

    private static <SR extends SearchStatusResponse> void sendFinalStatusResponse(
        GetAsyncStatusRequest request,
        SR response,
        ActionListener<SR> listener) {
        if (response.getExpirationTime() < System.currentTimeMillis()) { // check if the result has expired
            listener.onFailure(new ResourceNotFoundException(request.getId()));
        } else {
            listener.onResponse(response);
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

    /**
     * Encode the provided response in a binary form using base64 encoding.
     */
    String encodeResponse(R response) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Version.writeVersion(Version.CURRENT, out);
            response.writeTo(out);
            return Base64.getEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
        }
    }

    /**
     * Decode the provided base-64 bytes into a {@link AsyncSearchResponse}.
     */
    R decodeResponse(String value) throws IOException {
        try (ByteBufferStreamInput buf = new ByteBufferStreamInput(ByteBuffer.wrap(Base64.getDecoder().decode(value)))) {
            try (StreamInput in = new NamedWriteableAwareStreamInput(buf, registry)) {
                in.setVersion(Version.readVersion(in));
                return reader.read(in);
            }
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
}
