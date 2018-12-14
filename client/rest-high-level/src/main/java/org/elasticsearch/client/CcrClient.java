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
import org.elasticsearch.client.ccr.CcrStatsRequest;
import org.elasticsearch.client.ccr.CcrStatsResponse;
import org.elasticsearch.client.ccr.DeleteAutoFollowPatternRequest;
import org.elasticsearch.client.ccr.FollowStatsRequest;
import org.elasticsearch.client.ccr.FollowStatsResponse;
import org.elasticsearch.client.ccr.GetAutoFollowPatternRequest;
import org.elasticsearch.client.ccr.GetAutoFollowPatternResponse;
import org.elasticsearch.client.ccr.PauseFollowRequest;
import org.elasticsearch.client.ccr.PutAutoFollowPatternRequest;
import org.elasticsearch.client.ccr.PutFollowRequest;
import org.elasticsearch.client.ccr.PutFollowResponse;
import org.elasticsearch.client.ccr.ResumeFollowRequest;
import org.elasticsearch.client.ccr.UnfollowRequest;
import org.elasticsearch.client.core.AcknowledgedResponse;

import java.io.IOException;
import java.util.Collections;

/**
 * A wrapper for the {@link RestHighLevelClient} that provides methods for
 * accessing the Elastic ccr related methods
 * <p>
 * See the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-apis.html">
 * X-Pack Rollup APIs on elastic.co</a> for more information.
 */
public final class CcrClient {

    private final RestHighLevelClient restHighLevelClient;

    CcrClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Executes the put follow api, which creates a follower index and then the follower index starts following
     * the leader index.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-put-follow.html">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public PutFollowResponse putFollow(PutFollowRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            CcrRequestConverters::putFollow,
            options,
            PutFollowResponse::fromXContent,
            Collections.emptySet()
        );
    }

    /**
     * Asynchronously executes the put follow api, which creates a follower index and then the follower index starts
     * following the leader index.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-put-follow.html">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void putFollowAsync(PutFollowRequest request,
                               RequestOptions options,
                               ActionListener<PutFollowResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            CcrRequestConverters::putFollow,
            options,
            PutFollowResponse::fromXContent,
            listener,
            Collections.emptySet()
        );
    }

    /**
     * Instructs a follower index to pause the following of a leader index.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-pause-follow.html">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse pauseFollow(PauseFollowRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            CcrRequestConverters::pauseFollow,
            options,
            AcknowledgedResponse::fromXContent,
            Collections.emptySet()
        );
    }

    /**
     * Asynchronously instruct a follower index to pause the following of a leader index.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-pause-follow.html">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void pauseFollowAsync(PauseFollowRequest request,
                                 RequestOptions options,
                                 ActionListener<AcknowledgedResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            CcrRequestConverters::pauseFollow,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Instructs a follower index to resume the following of a leader index.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-resume-follow.html">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse resumeFollow(ResumeFollowRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            CcrRequestConverters::resumeFollow,
            options,
            AcknowledgedResponse::fromXContent,
            Collections.emptySet()
        );
    }

    /**
     * Asynchronously instruct a follower index to resume the following of a leader index.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-resume-follow.html">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void resumeFollowAsync(ResumeFollowRequest request,
                                  RequestOptions options,
                                  ActionListener<AcknowledgedResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            CcrRequestConverters::resumeFollow,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Instructs a follower index to unfollow and become a regular index.
     * Note that index following needs to be paused and the follower index needs to be closed.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-unfollow.html">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse unfollow(UnfollowRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            CcrRequestConverters::unfollow,
            options,
            AcknowledgedResponse::fromXContent,
            Collections.emptySet()
        );
    }

    /**
     * Asynchronously instructs a follower index to unfollow and become a regular index.
     * Note that index following needs to be paused and the follower index needs to be closed.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-unfollow.html">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void unfollowAsync(UnfollowRequest request,
                              RequestOptions options,
                              ActionListener<AcknowledgedResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            CcrRequestConverters::unfollow,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            Collections.emptySet()
        );
    }

    /**
     * Stores an auto follow pattern.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-put-auto-follow-pattern.html">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse putAutoFollowPattern(PutAutoFollowPatternRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            CcrRequestConverters::putAutoFollowPattern,
            options,
            AcknowledgedResponse::fromXContent,
            Collections.emptySet()
        );
    }

    /**
     * Asynchronously stores an auto follow pattern.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-put-auto-follow-pattern.html">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void putAutoFollowPatternAsync(PutAutoFollowPatternRequest request,
                                          RequestOptions options,
                                          ActionListener<AcknowledgedResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            CcrRequestConverters::putAutoFollowPattern,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Deletes an auto follow pattern.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-delete-auto-follow-pattern.html">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse deleteAutoFollowPattern(DeleteAutoFollowPatternRequest request,
                                                        RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            CcrRequestConverters::deleteAutoFollowPattern,
            options,
            AcknowledgedResponse::fromXContent,
            Collections.emptySet()
        );
    }

    /**
     * Asynchronously deletes an auto follow pattern.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-delete-auto-follow-pattern.html">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void deleteAutoFollowPatternAsync(DeleteAutoFollowPatternRequest request,
                                             RequestOptions options,
                                             ActionListener<AcknowledgedResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            CcrRequestConverters::deleteAutoFollowPattern,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            Collections.emptySet()
        );
    }

    /**
     * Gets an auto follow pattern.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-get-auto-follow-pattern.html">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetAutoFollowPatternResponse getAutoFollowPattern(GetAutoFollowPatternRequest request,
                                                             RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            CcrRequestConverters::getAutoFollowPattern,
            options,
            GetAutoFollowPatternResponse::fromXContent,
            Collections.emptySet()
        );
    }

    /**
     * Asynchronously gets an auto follow pattern.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-get-auto-follow-pattern.html">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void getAutoFollowPatternAsync(GetAutoFollowPatternRequest request,
                                          RequestOptions options,
                                          ActionListener<GetAutoFollowPatternResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            CcrRequestConverters::getAutoFollowPattern,
            options,
            GetAutoFollowPatternResponse::fromXContent,
            listener,
            Collections.emptySet()
        );
    }

    /**
     * Gets all CCR stats.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-get-stats.html">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public CcrStatsResponse getCcrStats(CcrStatsRequest request,
                                        RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            CcrRequestConverters::getCcrStats,
            options,
            CcrStatsResponse::fromXContent,
            Collections.emptySet()
        );
    }

    /**
     * Asynchronously gets all CCR stats.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-get-stats.html">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     */
    public void getCcrStatsAsync(CcrStatsRequest request,
                                 RequestOptions options,
                                 ActionListener<CcrStatsResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            CcrRequestConverters::getCcrStats,
            options,
            CcrStatsResponse::fromXContent,
            listener,
            Collections.emptySet()
        );
    }

    /**
     * Gets follow stats for specific indices.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-get-follow-stats.html">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public FollowStatsResponse getFollowStats(FollowStatsRequest request,
                                              RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            CcrRequestConverters::getFollowStats,
            options,
            FollowStatsResponse::fromXContent,
            Collections.emptySet()
        );
    }

    /**
     * Asynchronously gets follow stats for specific indices.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-get-follow-stats.html">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     */
    public void getFollowStatsAsync(FollowStatsRequest request,
                                    RequestOptions options,
                                    ActionListener<FollowStatsResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            CcrRequestConverters::getFollowStats,
            options,
            FollowStatsResponse::fromXContent,
            listener,
            Collections.emptySet()
        );
    }

}
