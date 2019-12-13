/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.PartialSearchResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.Random;

import static org.elasticsearch.xpack.search.AsyncSearchTemplateRegistry.INDEX_TEMPLATE_VERSION;
import static org.elasticsearch.xpack.search.AsyncSearchTemplateRegistry.ASYNC_SEARCH_TEMPLATE_NAME;

/**
 * A class that encapsulates the logic to store and retrieve {@link AsyncSearchResponse} to/from the .async-search index.
 */
class AsyncSearchStoreService {
    static final String ASYNC_SEARCH_ALIAS = ASYNC_SEARCH_TEMPLATE_NAME + "-" + INDEX_TEMPLATE_VERSION;
    static final String RESULT_FIELD = "result";

    private final Client client;
    private final NamedWriteableRegistry registry;
    private final Random random;

    AsyncSearchStoreService(Client client, NamedWriteableRegistry registry) {
        this.client = client;
        this.registry = registry;
        this.random = new Random(System.nanoTime());
    }

    /**
     * Store an empty document in the .async-search index that is used
     * as a place-holder for the future response.
     */
    void storeInitialResponse(ActionListener<IndexResponse> next) {
        IndexRequest request = new IndexRequest(ASYNC_SEARCH_ALIAS)
            .id(UUIDs.randomBase64UUID(random))
            .source(Collections.emptyMap(), XContentType.JSON);
        client.index(request, next);
    }

    /**
     * Store the final response if the place-holder document is still present (update).
     */
    void storeFinalResponse(AsyncSearchResponse response, ActionListener<UpdateResponse> next) throws IOException {
        AsyncSearchId searchId = AsyncSearchId.decode(response.id());
        UpdateRequest request = new UpdateRequest().index(searchId.getIndexName()).id(searchId.getDocId())
            .doc(Collections.singletonMap(RESULT_FIELD, encodeResponse(response)), XContentType.JSON)
            .detectNoop(false);
        client.update(request, next);
    }

    /**
     * Get the final response from the .async-search index if present, or delegate a {@link ResourceNotFoundException}
     * failure to the provided listener if not.
     */
    void getResponse(GetAsyncSearchAction.Request request, AsyncSearchId searchId, ActionListener<AsyncSearchResponse> next) {
        GetRequest internalGet = new GetRequest(searchId.getIndexName())
            .id(searchId.getDocId())
            .routing(searchId.getDocId());
        client.get(internalGet, ActionListener.wrap(
            get -> {
                if (get.isExists() == false) {
                    next.onFailure(new ResourceNotFoundException(request.getId() + " not found"));
                } else if (get.getSource().containsKey(RESULT_FIELD) == false) {
                    next.onResponse(new AsyncSearchResponse(request.getId(), new PartialSearchResponse(-1), 0, false));
                } else {
                    String encoded = (String) get.getSource().get(RESULT_FIELD);
                    next.onResponse(decodeResponse(encoded, registry));
                }
            },
            exc -> next.onFailure(new ResourceNotFoundException(request.getId() + " not found"))
        ));
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
