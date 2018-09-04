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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.indexlifecycle.DeleteLifecyclePolicyRequest;
import org.elasticsearch.client.indexlifecycle.LifecycleManagementStatusRequest;
import org.elasticsearch.client.indexlifecycle.LifecycleManagementStatusResponse;
import org.elasticsearch.client.indexlifecycle.PutLifecyclePolicyRequest;
import org.elasticsearch.protocol.xpack.indexlifecycle.ExplainLifecycleRequest;
import org.elasticsearch.protocol.xpack.indexlifecycle.ExplainLifecycleResponse;
import org.elasticsearch.protocol.xpack.indexlifecycle.SetIndexLifecyclePolicyRequest;
import org.elasticsearch.protocol.xpack.indexlifecycle.SetIndexLifecyclePolicyResponse;
import org.elasticsearch.protocol.xpack.indexlifecycle.StartILMRequest;
import org.elasticsearch.protocol.xpack.indexlifecycle.StopILMRequest;

import java.io.IOException;

import static java.util.Collections.emptySet;

public class IndexLifecycleClient {
    private final RestHighLevelClient restHighLevelClient;

    IndexLifecycleClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Create or modify a lifecycle definition
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse putLifecyclePolicy(PutLifecyclePolicyRequest request,
                                                   RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, RequestConverters::putLifecyclePolicy, options,
            AcknowledgedResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously create or modify a lifecycle definition
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void putLifecyclePolicyAsync(PutLifecyclePolicyRequest request, RequestOptions options,
                                        ActionListener<AcknowledgedResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request, RequestConverters::putLifecyclePolicy, options,
            AcknowledgedResponse::fromXContent, listener, emptySet());
    }

    /**
     * Delete a lifecycle definition
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse deleteLifecyclePolicy(DeleteLifecyclePolicyRequest request,
                                                      RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, RequestConverters::deleteLifecyclePolicy, options,
            AcknowledgedResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously delete a lifecycle definition
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void deleteLifecyclePolicyAsync(DeleteLifecyclePolicyRequest request, RequestOptions options,
                                           ActionListener<AcknowledgedResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request, RequestConverters::deleteLifecyclePolicy, options,
            AcknowledgedResponse::fromXContent, listener, emptySet());
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

    /**
     * Start the Index Lifecycle Management feature.
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse startILM(StartILMRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, RequestConverters::startILM, options,
                AcknowledgedResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously start the Index Lifecycle Management feature.
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void startILMAsync(StartILMRequest request, RequestOptions options, ActionListener<AcknowledgedResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request, RequestConverters::startILM, options,
                AcknowledgedResponse::fromXContent, listener, emptySet());
    }

    /**
     * Stop the Index Lifecycle Management feature.
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse stopILM(StopILMRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, RequestConverters::stopILM, options,
                AcknowledgedResponse::fromXContent, emptySet());
    }

    /**
     * Get the status of index lifecycle management
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     */
    public LifecycleManagementStatusResponse lifecycleManagementStatus(LifecycleManagementStatusRequest request, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, RequestConverters::lifecycleManagementStatus, options,
            LifecycleManagementStatusResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously get the status of index lifecycle management
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     *
     * @param request  the request
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void lifecycleManagementStatusAsync(LifecycleManagementStatusRequest request, RequestOptions options,
                                               ActionListener<LifecycleManagementStatusResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request, RequestConverters::lifecycleManagementStatus, options,
            LifecycleManagementStatusResponse::fromXContent, listener, emptySet());
    }

    /**
     * Asynchronously stop the Index Lifecycle Management feature.
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void stopILMAsync(StopILMRequest request, RequestOptions options, ActionListener<AcknowledgedResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request, RequestConverters::stopILM, options,
                AcknowledgedResponse::fromXContent, listener, emptySet());
    }

    /**
     * Explain the lifecycle state for an index
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public ExplainLifecycleResponse explainLifecycle(ExplainLifecycleRequest request,RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, RequestConverters::explainLifecycle, options,
            ExplainLifecycleResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously explain the lifecycle state for an index
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void explainLifecycleAsync(ExplainLifecycleRequest request, RequestOptions options,
                                      ActionListener<ExplainLifecycleResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request, RequestConverters::explainLifecycle, options,
                ExplainLifecycleResponse::fromXContent, listener, emptySet());
    }
}
