/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
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
import org.elasticsearch.common.bytes.BytesReference;
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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
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

    private static Settings settings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .build();
    }

    private static XContentBuilder mappings() throws IOException {
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
    }

    private final String index;
    private final ClusterService clusterService;
    private final Client client;
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
        this.clusterService = clusterService;
        this.securityContext = new SecurityContext(clusterService.getSettings(), threadContext);
        this.client = new OriginSettingClient(client, origin);
        this.registry = registry;
        this.reader = reader;
    }

    /**
     * Returns the internal client with origin.
     */
    public Client getClient() {
        return client;
    }

    /**
     * Creates the index with the expected settings and mappings if it doesn't exist.
     */
    void createIndexIfNecessary(ActionListener<Void> listener) {
        if (clusterService.state().routingTable().hasIndex(index) == false) {
            try {
                client.admin().indices().prepareCreate(index)
                    .setSettings(settings())
                    .setMapping(mappings())
                    .execute(ActionListener.wrap(
                        resp -> listener.onResponse(null),
                        exc -> {
                            if (ExceptionsHelper.unwrapCause(exc) instanceof ResourceAlreadyExistsException) {
                                listener.onResponse(null);
                            } else {
                                logger.error("failed to create " + index + " index", exc);
                                listener.onFailure(exc);
                            }
                        }));
            } catch (Exception exc) {
                logger.error("failed to create " + index + " index", exc);
                listener.onFailure(exc);
            }
        } else {
            listener.onResponse(null);
        }
    }

    /**
     * Stores the initial response with the original headers of the authenticated user
     * and the expected expiration time.
     */
    public void storeInitialResponse(String docId,
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
        createIndexIfNecessary(ActionListener.wrap(v -> client.index(indexRequest, listener), listener::onFailure));
    }

    /**
     * Stores the final response if the place-holder document is still present (update).
     */
    public void storeFinalResponse(String docId,
                            Map<String, List<String>> responseHeaders,
                            R response,
                            ActionListener<UpdateResponse> listener) throws IOException {
        Map<String, Object> source = new HashMap<>();
        source.put(RESPONSE_HEADERS_FIELD, responseHeaders);
        source.put(RESULT_FIELD, encodeResponse(response));
        UpdateRequest request = new UpdateRequest()
            .index(index)
            .id(docId)
            .doc(source, XContentType.JSON);
        client.update(request, listener);
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
            .doc(source, XContentType.JSON);
        client.update(request, listener);
    }

    /**
     * Deletes the provided <code>asyncTaskId</code> from the index if present.
     */
    public void deleteResponse(AsyncExecutionId asyncExecutionId,
                               ActionListener<DeleteResponse> listener) {
        DeleteRequest request = new DeleteRequest(index).id(asyncExecutionId.getDocId());
        client.delete(request, listener);
    }

    /**
     * Returns the {@link AsyncTask} if the provided <code>asyncTaskId</code>
     * is registered in the task manager, <code>null</code> otherwise.
     *
     * This method throws a {@link ResourceNotFoundException} if the authenticated user
     * is not the creator of the original task.
     */
    public <T extends AsyncTask> T getTask(TaskManager taskManager, AsyncExecutionId asyncExecutionId, Class<T> tClass) throws IOException {
        Task task = taskManager.getTask(asyncExecutionId.getTaskId().getId());
        if (tClass.isInstance(task) == false) {
            return null;
        }
        @SuppressWarnings("unchecked") T asyncTask = (T) task;
        if (asyncTask.getExecutionId().equals(asyncExecutionId) == false) {
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
        final Authentication current = securityContext.getAuthentication();
        GetRequest internalGet = new GetRequest(index)
            .preference(asyncExecutionId.getEncoded())
            .id(asyncExecutionId.getDocId());
        client.get(internalGet, ActionListener.wrap(
            get -> {
                if (get.isExists() == false) {
                    listener.onFailure(new ResourceNotFoundException(asyncExecutionId.getEncoded()));
                    return;
                }

                // check the authentication of the current user against the user that initiated the async task
                @SuppressWarnings("unchecked")
                Map<String, String> headers = (Map<String, String>) get.getSource().get(HEADERS_FIELD);
                if (ensureAuthenticatedUserIsSame(headers, current) == false) {
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
                    listener.onResponse(decodeResponse(encoded).withExpirationTime(expirationTime));
                } else {
                    listener.onResponse(null);
                }
            },
            listener::onFailure
        ));
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
        return ensureAuthenticatedUserIsSame(origin, current);
    }

    /**
     * Compares the {@link Authentication} that was used to create the {@link AsyncExecutionId} with the
     * current authentication.
     */
    boolean ensureAuthenticatedUserIsSame(Authentication original, Authentication current) {
        final boolean samePrincipal = original.getUser().principal().equals(current.getUser().principal());
        final boolean sameRealmType;
        if (original.getUser().isRunAs()) {
            if (current.getUser().isRunAs()) {
                sameRealmType = original.getLookedUpBy().getType().equals(current.getLookedUpBy().getType());
            }  else {
                sameRealmType = original.getLookedUpBy().getType().equals(current.getAuthenticatedBy().getType());
            }
        } else if (current.getUser().isRunAs()) {
            sameRealmType = original.getAuthenticatedBy().getType().equals(current.getLookedUpBy().getType());
        } else {
            sameRealmType = original.getAuthenticatedBy().getType().equals(current.getAuthenticatedBy().getType());
        }
        return samePrincipal && sameRealmType;
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
