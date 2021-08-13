/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.ccr.CcrStatsRequest;
import org.elasticsearch.client.ccr.CcrStatsResponse;
import org.elasticsearch.client.ccr.DeleteAutoFollowPatternRequest;
import org.elasticsearch.client.ccr.FollowInfoRequest;
import org.elasticsearch.client.ccr.FollowInfoResponse;
import org.elasticsearch.client.ccr.FollowStatsRequest;
import org.elasticsearch.client.ccr.FollowStatsResponse;
import org.elasticsearch.client.ccr.ForgetFollowerRequest;
import org.elasticsearch.client.ccr.GetAutoFollowPatternRequest;
import org.elasticsearch.client.ccr.GetAutoFollowPatternResponse;
import org.elasticsearch.client.ccr.PauseAutoFollowPatternRequest;
import org.elasticsearch.client.ccr.PauseFollowRequest;
import org.elasticsearch.client.ccr.PutAutoFollowPatternRequest;
import org.elasticsearch.client.ccr.PutFollowRequest;
import org.elasticsearch.client.ccr.PutFollowResponse;
import org.elasticsearch.client.ccr.ResumeAutoFollowPatternRequest;
import org.elasticsearch.client.ccr.ResumeFollowRequest;
import org.elasticsearch.client.ccr.UnfollowRequest;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.core.BroadcastResponse;

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
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putFollowAsync(PutFollowRequest request,
                                      RequestOptions options,
                                      ActionListener<PutFollowResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
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
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable pauseFollowAsync(PauseFollowRequest request,
                                        RequestOptions options,
                                        ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
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
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable resumeFollowAsync(ResumeFollowRequest request,
                                         RequestOptions options,
                                         ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
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
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable unfollowAsync(UnfollowRequest request,
                                     RequestOptions options,
                                     ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            CcrRequestConverters::unfollow,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            Collections.emptySet()
        );
    }

    /**
     * Instructs an index acting as a leader index to forget the specified follower index.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-forget-follower.html">the docs</a> for more details
     * on the intended usage of this API.
     *
     * @param request the request
     * @param options the request options (e.g., headers), use {@link RequestOptions#DEFAULT} if the defaults are acceptable.
     * @return the response
     * @throws IOException if an I/O exception occurs while executing this request
     */
    public BroadcastResponse forgetFollower(final ForgetFollowerRequest request, final RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
                request,
                CcrRequestConverters::forgetFollower,
                options,
                BroadcastResponse::fromXContent,
                Collections.emptySet());
    }

    /**
     * Asynchronously instructs an index acting as a leader index to forget the specified follower index.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-forget-follower.html">the docs</a> for more details
     * on the intended usage of this API.
     * @param request the request
     * @param options the request options (e.g., headers), use {@link RequestOptions#DEFAULT} if the defaults are acceptable.
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable forgetFollowerAsync(
            final ForgetFollowerRequest request,
            final RequestOptions options,
            final ActionListener<BroadcastResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
                request,
                CcrRequestConverters::forgetFollower,
                options,
                BroadcastResponse::fromXContent,
                listener,
                Collections.emptySet());
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
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putAutoFollowPatternAsync(PutAutoFollowPatternRequest request,
                                                 RequestOptions options,
                                                 ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
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
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteAutoFollowPatternAsync(DeleteAutoFollowPatternRequest request,
                                                    RequestOptions options,
                                                    ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
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
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getAutoFollowPatternAsync(GetAutoFollowPatternRequest request,
                                                 RequestOptions options,
                                                 ActionListener<GetAutoFollowPatternResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            CcrRequestConverters::getAutoFollowPattern,
            options,
            GetAutoFollowPatternResponse::fromXContent,
            listener,
            Collections.emptySet()
        );
    }

    /**
     * Pauses an auto follow pattern.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-pause-auto-follow-pattern.html">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse pauseAutoFollowPattern(PauseAutoFollowPatternRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            CcrRequestConverters::pauseAutoFollowPattern,
            options,
            AcknowledgedResponse::fromXContent,
            Collections.emptySet()
        );
    }

    /**
     * Asynchronously pauses an auto follow pattern.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-pause-auto-follow-pattern.html">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable pauseAutoFollowPatternAsync(PauseAutoFollowPatternRequest request,
                                                   RequestOptions options,
                                                   ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            CcrRequestConverters::pauseAutoFollowPattern,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            Collections.emptySet());
    }

    /**
     * Resumes an auto follow pattern.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-resume-auto-follow-pattern.html">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse resumeAutoFollowPattern(ResumeAutoFollowPatternRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            CcrRequestConverters::resumeAutoFollowPattern,
            options,
            AcknowledgedResponse::fromXContent,
            Collections.emptySet()
        );
    }

    /**
     * Asynchronously resumes an auto follow pattern.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-resume-auto-follow-pattern.html">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable resumeAutoFollowPatternAsync(ResumeAutoFollowPatternRequest request,
                                                    RequestOptions options,
                                                    ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            CcrRequestConverters::resumeAutoFollowPattern,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            Collections.emptySet());
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
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getCcrStatsAsync(CcrStatsRequest request,
                                        RequestOptions options,
                                        ActionListener<CcrStatsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
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
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getFollowStatsAsync(FollowStatsRequest request,
                                           RequestOptions options,
                                           ActionListener<FollowStatsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            CcrRequestConverters::getFollowStats,
            options,
            FollowStatsResponse::fromXContent,
            listener,
            Collections.emptySet()
        );
    }

    /**
     * Gets follow info for specific indices.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-get-follow-info.html">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public FollowInfoResponse getFollowInfo(FollowInfoRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            CcrRequestConverters::getFollowInfo,
            options,
            FollowInfoResponse::fromXContent,
            Collections.emptySet()
        );
    }

    /**
     * Asynchronously gets follow info for specific indices.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-get-follow-info.html">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getFollowInfoAsync(FollowInfoRequest request,
                                          RequestOptions options,
                                          ActionListener<FollowInfoResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            CcrRequestConverters::getFollowInfo,
            options,
            FollowInfoResponse::fromXContent,
            listener,
            Collections.emptySet()
        );
    }
}
