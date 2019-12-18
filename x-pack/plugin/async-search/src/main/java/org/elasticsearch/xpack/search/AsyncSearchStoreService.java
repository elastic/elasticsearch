/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.elasticsearch.xpack.core.ClientHelper.INDEX_LIFECYCLE_ORIGIN;
import static org.elasticsearch.xpack.search.AsyncSearchTemplateRegistry.INDEX_TEMPLATE_VERSION;
import static org.elasticsearch.xpack.search.AsyncSearchTemplateRegistry.ASYNC_SEARCH_TEMPLATE_NAME;
import static org.elasticsearch.xpack.search.TransportGetAsyncSearchAction.ensureAuthenticatedUserIsSame;

/**
 * A class that encapsulates the logic to store and retrieve {@link AsyncSearchResponse} to/from the .async-search index.
 */
class AsyncSearchStoreService {
    private static final Logger logger = LogManager.getLogger(AsyncSearchStoreService.class);

    static final String ASYNC_SEARCH_ALIAS = ASYNC_SEARCH_TEMPLATE_NAME + "-" + INDEX_TEMPLATE_VERSION;
    static final String RESULT_FIELD = "result";
    static final String HEADERS_FIELD = "headers";

    private final Client client;
    private final NamedWriteableRegistry registry;
    private final Random random;

    AsyncSearchStoreService(Client client, NamedWriteableRegistry registry) {
        this.client = new OriginSettingClient(client, INDEX_LIFECYCLE_ORIGIN);
        this.registry = registry;
        this.random = new Random(System.nanoTime());
    }

    /**
     * Store an empty document in the .async-search index that is used
     * as a place-holder for the future response.
     */
    void storeInitialResponse(Map<String, String> headers, ActionListener<IndexResponse> listener) {
        IndexRequest request = new IndexRequest(ASYNC_SEARCH_ALIAS)
            .id(UUIDs.randomBase64UUID(random))
            .source(Collections.singletonMap(HEADERS_FIELD, headers), XContentType.JSON);
        client.index(request, listener);
    }

    /**
     * Store the final response if the place-holder document is still present (update).
     */
    void storeFinalResponse(Map<String, String> headers, AsyncSearchResponse response,
                            ActionListener<UpdateResponse> listener) throws IOException {
        AsyncSearchId searchId = AsyncSearchId.decode(response.id());
        Map<String, Object> source = new HashMap<>();
        source.put(RESULT_FIELD, encodeResponse(response));
        source.put(HEADERS_FIELD, headers);
        UpdateRequest request = new UpdateRequest().index(searchId.getIndexName()).id(searchId.getDocId())
            .doc(source, XContentType.JSON)
            .detectNoop(false);
        client.update(request, listener);
    }

    /**
     * Get the response from the .async-search index if present, or delegate a {@link ResourceNotFoundException}
     * failure to the provided listener if not.
     */
    void getResponse(AsyncSearchId searchId, ActionListener<AsyncSearchResponse> listener) {
        final Authentication current = Authentication.getAuthentication(client.threadPool().getThreadContext());
        GetRequest internalGet = new GetRequest(searchId.getIndexName())
            .preference(searchId.getEncoded())
            .id(searchId.getDocId());
        client.get(internalGet, ActionListener.wrap(
            get -> {
                if (get.isExists() == false) {
                    listener.onFailure(new ResourceNotFoundException(searchId.getEncoded() + " not found"));
                    return;
                }

                // check the authentication of the current user against the user that initiated the async search
                @SuppressWarnings("unchecked")
                Map<String, String> headers = (Map<String, String>) get.getSource().get(HEADERS_FIELD);
                if (ensureAuthenticatedUserIsSame(headers, current) == false) {
                    listener.onFailure(new ResourceNotFoundException(searchId.getEncoded() + " not found"));
                    return;
                }

                @SuppressWarnings("unchecked")
                String encoded = (String) get.getSource().get(RESULT_FIELD);
                listener.onResponse(encoded != null ? decodeResponse(encoded, registry) : null);
            },
            listener::onFailure
        ));
    }

    void deleteResult(AsyncSearchId searchId, ActionListener<AcknowledgedResponse> listener) {
        DeleteRequest request = new DeleteRequest(searchId.getIndexName()).id(searchId.getDocId());
        client.delete(request, ActionListener.wrap(
            resp -> {
                if (resp.status() == RestStatus.NOT_FOUND) {
                    listener.onFailure(new ResourceNotFoundException("id [{}] not found", searchId.getEncoded()));
                } else {
                    listener.onResponse(new AcknowledgedResponse(true));
                }
            },
            exc -> {
                logger.error(() -> new ParameterizedMessage("failed to clean async-search [{}]", searchId.getEncoded()), exc);
                listener.onFailure(new ResourceNotFoundException("id [{}] not found", searchId.getEncoded()));
            })
        );
    }

    /**
     * Encode the provided response in a binary form using base64 encoding.
     */
    static String encodeResponse(AsyncSearchResponse response) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Version.writeVersion(Version.CURRENT, out);
            response.writeTo(out);
            return Base64.getEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
        }
    }

    /**
     * Decode the provided base-64 bytes into a {@link AsyncSearchResponse}.
     */
    static AsyncSearchResponse decodeResponse(String value, NamedWriteableRegistry registry) throws IOException {
        try (ByteBufferStreamInput buf = new ByteBufferStreamInput(ByteBuffer.wrap(Base64.getDecoder().decode(value)))) {
            try (StreamInput in = new NamedWriteableAwareStreamInput(buf, registry)) {
                in.setVersion(Version.readVersion(in));
                return new AsyncSearchResponse(in);
            }
        }
    }
}
