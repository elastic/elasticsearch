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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.protocol.xpack.watcher.PutWatchRequest;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;

import java.io.IOException;

import static java.util.Collections.emptySet;

public final class WatcherClient {

    private final RestHighLevelClient restHighLevelClient;

    WatcherClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Put a watch into the cluster
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/watcher-api-put-watch.html">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public PutWatchResponse putWatch(PutWatchRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, RequestConverters::xPackWatcherPutWatch, options,
            PutWatchResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously put a watch into the cluster
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/watcher-api-put-watch.html">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void putWatchAsync(PutWatchRequest request, RequestOptions options,
                              ActionListener<PutWatchResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request, RequestConverters::xPackWatcherPutWatch, options,
            PutWatchResponse::fromXContent, listener, emptySet());
    }
}
