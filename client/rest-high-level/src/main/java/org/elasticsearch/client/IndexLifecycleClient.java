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
import org.elasticsearch.protocol.xpack.indexlifecycle.SetIndexLifecyclePolicyRequest;
import org.elasticsearch.protocol.xpack.indexlifecycle.SetIndexLifecyclePolicyResponse;

import java.io.IOException;

import static java.util.Collections.emptySet;

public class IndexLifecycleClient {
    private final RestHighLevelClient restHighLevelClient;

    IndexLifecycleClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Set the index lifecycle policy for an index
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public SetIndexLifecyclePolicyResponse setIndexLifecyclePolicy(SetIndexLifecyclePolicyRequest request,
                                                                   RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, RequestConverters::setIndexLifecyclePolicy, options,
            SetIndexLifecyclePolicyResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously set the index lifecycle policy for an index
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void setIndexLifecyclePolicyAsync(SetIndexLifecyclePolicyRequest request, RequestOptions options,
                                             ActionListener<SetIndexLifecyclePolicyResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request, RequestConverters::setIndexLifecyclePolicy, options,
            SetIndexLifecyclePolicyResponse::fromXContent, listener, emptySet());
    }
}
