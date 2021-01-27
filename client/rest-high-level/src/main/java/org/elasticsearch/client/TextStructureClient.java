/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
