/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client;

import java.io.IOException;
import java.util.Collections;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.textstructure.FindStructureRequest;
import org.elasticsearch.client.textstructure.FindStructureResponse;


/**
 * Text Structure API client wrapper for the {@link RestHighLevelClient}
 * <p>
 * See the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/find-structure.html">
 * X-Pack Text Structure APIs </a> for additional information.
 */
public final class TextStructureClient {

    private final RestHighLevelClient restHighLevelClient;

    TextStructureClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Finds the structure of a file
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/find-structure.html">
     *     Text Structure Find Structure documentation</a>
     *
     * @param request The find file structure request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response containing details of the file structure
     * @throws IOException when there is a serialization issue sending the request or receiving the response
     */
    public FindStructureResponse findStructure(FindStructureRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request,
            TextStructureRequestConverters::findFileStructure,
            options,
            FindStructureResponse::fromXContent,
            Collections.emptySet());
    }

    /**
     * Finds the structure of a file asynchronously and notifies the listener on completion
     * <p>
     * For additional info
     * see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/find-structure.html">
     *         Text Structure Find Structure documentation</a>
     *
     * @param request The find file structure request
     * @param options  Additional request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener Listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable findStructureAsync(FindStructureRequest request, RequestOptions options,
                                          ActionListener<FindStructureResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            TextStructureRequestConverters::findFileStructure,
            options,
            FindStructureResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

}
