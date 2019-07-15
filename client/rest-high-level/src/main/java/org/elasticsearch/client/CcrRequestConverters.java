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

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.ccr.CcrStatsRequest;
import org.elasticsearch.client.ccr.DeleteAutoFollowPatternRequest;
import org.elasticsearch.client.ccr.FollowInfoRequest;
import org.elasticsearch.client.ccr.FollowStatsRequest;
import org.elasticsearch.client.ccr.ForgetFollowerRequest;
import org.elasticsearch.client.ccr.GetAutoFollowPatternRequest;
import org.elasticsearch.client.ccr.PauseFollowRequest;
import org.elasticsearch.client.ccr.PutAutoFollowPatternRequest;
import org.elasticsearch.client.ccr.PutFollowRequest;
import org.elasticsearch.client.ccr.ResumeFollowRequest;
import org.elasticsearch.client.ccr.UnfollowRequest;

import java.io.IOException;

import static org.elasticsearch.client.RequestConverters.REQUEST_BODY_CONTENT_TYPE;
import static org.elasticsearch.client.RequestConverters.createEntity;

final class CcrRequestConverters {

    static Request putFollow(PutFollowRequest putFollowRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPart(putFollowRequest.getFollowerIndex())
            .addPathPartAsIs("_ccr", "follow")
            .build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withWaitForActiveShards(putFollowRequest.waitForActiveShards());
        request.setEntity(createEntity(putFollowRequest, REQUEST_BODY_CONTENT_TYPE));
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request pauseFollow(PauseFollowRequest pauseFollowRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPart(pauseFollowRequest.getFollowerIndex())
            .addPathPartAsIs("_ccr", "pause_follow")
            .build();
        return new Request(HttpPost.METHOD_NAME, endpoint);
    }

    static Request resumeFollow(ResumeFollowRequest resumeFollowRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPart(resumeFollowRequest.getFollowerIndex())
            .addPathPartAsIs("_ccr", "resume_follow")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(resumeFollowRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request unfollow(UnfollowRequest unfollowRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPart(unfollowRequest.getFollowerIndex())
            .addPathPartAsIs("_ccr", "unfollow")
            .build();
        return new Request(HttpPost.METHOD_NAME, endpoint);
    }

    static Request forgetFollower(final ForgetFollowerRequest forgetFollowerRequest) throws IOException {
        final String endpoint = new RequestConverters.EndpointBuilder()
                .addPathPart(forgetFollowerRequest.leaderIndex())
                .addPathPartAsIs("_ccr")
                .addPathPartAsIs("forget_follower")
                .build();
        final Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(forgetFollowerRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request putAutoFollowPattern(PutAutoFollowPatternRequest putAutoFollowPatternRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_ccr", "auto_follow")
            .addPathPart(putAutoFollowPatternRequest.getName())
            .build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        request.setEntity(createEntity(putAutoFollowPatternRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request deleteAutoFollowPattern(DeleteAutoFollowPatternRequest deleteAutoFollowPatternRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_ccr", "auto_follow")
            .addPathPart(deleteAutoFollowPatternRequest.getName())
            .build();
        return new Request(HttpDelete.METHOD_NAME, endpoint);
    }

    static Request getAutoFollowPattern(GetAutoFollowPatternRequest getAutoFollowPatternRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_ccr", "auto_follow")
            .addPathPart(getAutoFollowPatternRequest.getName())
            .build();
        return new Request(HttpGet.METHOD_NAME, endpoint);
    }

    static Request getCcrStats(CcrStatsRequest ccrStatsRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_ccr", "stats")
            .build();
        return new Request(HttpGet.METHOD_NAME, endpoint);
    }

    static Request getFollowStats(FollowStatsRequest followStatsRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPart(followStatsRequest.getFollowerIndex())
            .addPathPartAsIs("_ccr", "stats")
            .build();
        return new Request(HttpGet.METHOD_NAME, endpoint);
    }

    static Request getFollowInfo(FollowInfoRequest followInfoRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPart(followInfoRequest.getFollowerIndex())
            .addPathPartAsIs("_ccr", "info")
            .build();
        return new Request(HttpGet.METHOD_NAME, endpoint);
    }

}
