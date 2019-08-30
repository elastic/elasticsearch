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
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.ilm.DeleteLifecyclePolicyRequest;
import org.elasticsearch.client.ilm.ExplainLifecycleRequest;
import org.elasticsearch.client.ilm.ExplainLifecycleResponse;
import org.elasticsearch.client.ilm.GetLifecyclePolicyRequest;
import org.elasticsearch.client.ilm.GetLifecyclePolicyResponse;
import org.elasticsearch.client.ilm.LifecycleManagementStatusRequest;
import org.elasticsearch.client.ilm.LifecycleManagementStatusResponse;
import org.elasticsearch.client.ilm.PutLifecyclePolicyRequest;
import org.elasticsearch.client.ilm.RemoveIndexLifecyclePolicyRequest;
import org.elasticsearch.client.ilm.RemoveIndexLifecyclePolicyResponse;
import org.elasticsearch.client.ilm.RetryLifecyclePolicyRequest;
import org.elasticsearch.client.ilm.StartILMRequest;
import org.elasticsearch.client.ilm.StopILMRequest;
import org.elasticsearch.client.slm.DeleteSnapshotLifecyclePolicyRequest;
import org.elasticsearch.client.slm.ExecuteSnapshotLifecyclePolicyRequest;
import org.elasticsearch.client.slm.ExecuteSnapshotLifecyclePolicyResponse;
import org.elasticsearch.client.slm.GetSnapshotLifecyclePolicyRequest;
import org.elasticsearch.client.slm.GetSnapshotLifecyclePolicyResponse;
import org.elasticsearch.client.slm.PutSnapshotLifecyclePolicyRequest;

import java.io.IOException;

import static java.util.Collections.emptySet;

public class IndexLifecycleClient {
    private final RestHighLevelClient restHighLevelClient;

    IndexLifecycleClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Retrieve one or more lifecycle policy definition. See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-ilm-ilm-get-lifecycle-policy.html">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetLifecyclePolicyResponse getLifecyclePolicy(GetLifecyclePolicyRequest request,
                                                         RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, IndexLifecycleRequestConverters::getLifecyclePolicy, options,
            GetLifecyclePolicyResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously retrieve one or more lifecycle policy definition. See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-ilm-ilm-get-lifecycle-policy.html">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getLifecyclePolicyAsync(GetLifecyclePolicyRequest request, RequestOptions options,
                                               ActionListener<GetLifecyclePolicyResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, IndexLifecycleRequestConverters::getLifecyclePolicy, options,
            GetLifecyclePolicyResponse::fromXContent, listener, emptySet());
    }

    /**
     * Create or modify a lifecycle definition. See <a href=
     * "https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-ilm-ilm-put-lifecycle-policy.html">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse putLifecyclePolicy(PutLifecyclePolicyRequest request,
                                                   RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, IndexLifecycleRequestConverters::putLifecyclePolicy, options,
            AcknowledgedResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously create or modify a lifecycle definition. See <a href=
     * "https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-ilm-ilm-put-lifecycle-policy.html">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putLifecyclePolicyAsync(PutLifecyclePolicyRequest request, RequestOptions options,
                                               ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, IndexLifecycleRequestConverters::putLifecyclePolicy, options,
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
        return restHighLevelClient.performRequestAndParseEntity(request, IndexLifecycleRequestConverters::deleteLifecyclePolicy, options,
            AcknowledgedResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously delete a lifecycle definition
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteLifecyclePolicyAsync(DeleteLifecyclePolicyRequest request, RequestOptions options,
                                                  ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request, IndexLifecycleRequestConverters::deleteLifecyclePolicy, options,
            AcknowledgedResponse::fromXContent, listener, emptySet());
    }

    /**
     * Remove the index lifecycle policy for an index
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public RemoveIndexLifecyclePolicyResponse removeIndexLifecyclePolicy(RemoveIndexLifecyclePolicyRequest request,
            RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, IndexLifecycleRequestConverters::removeIndexLifecyclePolicy,
            options, RemoveIndexLifecyclePolicyResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously remove the index lifecycle policy for an index
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable removeIndexLifecyclePolicyAsync(RemoveIndexLifecyclePolicyRequest request, RequestOptions options,
                                                       ActionListener<RemoveIndexLifecyclePolicyResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request, IndexLifecycleRequestConverters::removeIndexLifecyclePolicy, options,
                RemoveIndexLifecyclePolicyResponse::fromXContent, listener, emptySet());
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
        return restHighLevelClient.performRequestAndParseEntity(request, IndexLifecycleRequestConverters::startILM, options,
                AcknowledgedResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously start the Index Lifecycle Management feature.
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable startILMAsync(StartILMRequest request, RequestOptions options, ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, IndexLifecycleRequestConverters::startILM, options,
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
        return restHighLevelClient.performRequestAndParseEntity(request, IndexLifecycleRequestConverters::stopILM, options,
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
        return restHighLevelClient.performRequestAndParseEntity(request, IndexLifecycleRequestConverters::lifecycleManagementStatus,
            options, LifecycleManagementStatusResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously get the status of index lifecycle management
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     * @param request  the request
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable lifecycleManagementStatusAsync(LifecycleManagementStatusRequest request, RequestOptions options,
                                                      ActionListener<LifecycleManagementStatusResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request, IndexLifecycleRequestConverters::lifecycleManagementStatus, options,
            LifecycleManagementStatusResponse::fromXContent, listener, emptySet());
    }

    /**
     * Asynchronously stop the Index Lifecycle Management feature.
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable stopILMAsync(StopILMRequest request, RequestOptions options, ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, IndexLifecycleRequestConverters::stopILM, options,
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
        return restHighLevelClient.performRequestAndParseEntity(request, IndexLifecycleRequestConverters::explainLifecycle, options,
            ExplainLifecycleResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously explain the lifecycle state for an index
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable explainLifecycleAsync(ExplainLifecycleRequest request, RequestOptions options,
                                             ActionListener<ExplainLifecycleResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, IndexLifecycleRequestConverters::explainLifecycle, options,
                ExplainLifecycleResponse::fromXContent, listener, emptySet());
    }

    /**
     * Retry lifecycle step for given indices
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse retryLifecyclePolicy(RetryLifecyclePolicyRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, IndexLifecycleRequestConverters::retryLifecycle, options,
            AcknowledgedResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously retry the lifecycle step for given indices
     * See <a href="https://fix-me-when-we-have-docs.com">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable retryLifecyclePolicyAsync(RetryLifecyclePolicyRequest request, RequestOptions options,
                                                 ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, IndexLifecycleRequestConverters::retryLifecycle, options,
            AcknowledgedResponse::fromXContent, listener, emptySet());
    }

    /**
     * Retrieve one or more snapshot lifecycle policy definitions.
     * See <pre>
     *  https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/
     *  java-rest-high-ilm-slm-get-snapshot-lifecycle-policy.html
     * </pre>
     * for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetSnapshotLifecyclePolicyResponse getSnapshotLifecyclePolicy(GetSnapshotLifecyclePolicyRequest request,
                                                                         RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, IndexLifecycleRequestConverters::getSnapshotLifecyclePolicy,
            options, GetSnapshotLifecyclePolicyResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously retrieve one or more snapshot lifecycle policy definition.
     * See <pre>
     *  https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/
     *  java-rest-high-ilm-slm-get-snapshot-lifecycle-policy.html
     * </pre>
     * for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getSnapshotLifecyclePolicyAsync(GetSnapshotLifecyclePolicyRequest request, RequestOptions options,
                                                       ActionListener<GetSnapshotLifecyclePolicyResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, IndexLifecycleRequestConverters::getSnapshotLifecyclePolicy,
            options, GetSnapshotLifecyclePolicyResponse::fromXContent, listener, emptySet());
    }

    /**
     * Create or modify a snapshot lifecycle definition.
     * See <pre>
     *  https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/
     *  java-rest-high-ilm-slm-put-snapshot-lifecycle-policy.html
     * </pre>
     * for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse putSnapshotLifecyclePolicy(PutSnapshotLifecyclePolicyRequest request,
                                                           RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, IndexLifecycleRequestConverters::putSnapshotLifecyclePolicy,
            options, AcknowledgedResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously create or modify a snapshot lifecycle definition.
     * See <pre>
     *  https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/
     *  java-rest-high-ilm-slm-put-snapshot-lifecycle-policy.html
     * </pre>
     * for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putSnapshotLifecyclePolicyAsync(PutSnapshotLifecyclePolicyRequest request, RequestOptions options,
                                                       ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, IndexLifecycleRequestConverters::putSnapshotLifecyclePolicy,
            options, AcknowledgedResponse::fromXContent, listener, emptySet());
    }

    /**
     * Delete a snapshot lifecycle definition
     * See <pre>
     *  https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/
     *  java-rest-high-ilm-slm-delete-snapshot-lifecycle-policy.html
     * </pre>
     * for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse deleteSnapshotLifecyclePolicy(DeleteSnapshotLifecyclePolicyRequest request,
                                                              RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, IndexLifecycleRequestConverters::deleteSnapshotLifecyclePolicy,
            options, AcknowledgedResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously delete a snapshot lifecycle definition
     * See <pre>
     *  https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/
     *  java-rest-high-ilm-slm-delete-snapshot-lifecycle-policy.html
     * </pre>
     * for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteSnapshotLifecyclePolicyAsync(DeleteSnapshotLifecyclePolicyRequest request,
                                          RequestOptions options,ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request, IndexLifecycleRequestConverters::deleteSnapshotLifecyclePolicy,
            options, AcknowledgedResponse::fromXContent, listener, emptySet());
    }

    /**
     * Execute a snapshot lifecycle definition
     * See <pre>
     *  https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/
     *  java-rest-high-ilm-slm-execute-snapshot-lifecycle-policy.html
     * </pre>
     * for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public ExecuteSnapshotLifecyclePolicyResponse executeSnapshotLifecyclePolicy(ExecuteSnapshotLifecyclePolicyRequest request,
                                                                                 RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, IndexLifecycleRequestConverters::executeSnapshotLifecyclePolicy,
            options, ExecuteSnapshotLifecyclePolicyResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously execute a snapshot lifecycle definition
     * See <pre>
     *  https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/
     *  java-rest-high-ilm-slm-execute-snapshot-lifecycle-policy.html
     * </pre>
     * for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable executeSnapshotLifecyclePolicyAsync(
        ExecuteSnapshotLifecyclePolicyRequest request, RequestOptions options,
        ActionListener<ExecuteSnapshotLifecyclePolicyResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request, IndexLifecycleRequestConverters::executeSnapshotLifecyclePolicy,
            options, ExecuteSnapshotLifecyclePolicyResponse::fromXContent, listener, emptySet());
    }
}
