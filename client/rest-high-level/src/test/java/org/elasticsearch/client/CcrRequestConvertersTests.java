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
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.ccr.CcrStatsRequest;
import org.elasticsearch.client.ccr.DeleteAutoFollowPatternRequest;
import org.elasticsearch.client.ccr.FollowConfig;
import org.elasticsearch.client.ccr.FollowInfoRequest;
import org.elasticsearch.client.ccr.FollowStatsRequest;
import org.elasticsearch.client.ccr.ForgetFollowerRequest;
import org.elasticsearch.client.ccr.GetAutoFollowPatternRequest;
import org.elasticsearch.client.ccr.PauseAutoFollowPatternRequest;
import org.elasticsearch.client.ccr.PauseFollowRequest;
import org.elasticsearch.client.ccr.PutAutoFollowPatternRequest;
import org.elasticsearch.client.ccr.PutFollowRequest;
import org.elasticsearch.client.ccr.ResumeAutoFollowPatternRequest;
import org.elasticsearch.client.ccr.ResumeFollowRequest;
import org.elasticsearch.client.ccr.UnfollowRequest;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class CcrRequestConvertersTests extends ESTestCase {

    public void testPutFollow() throws Exception {
        PutFollowRequest putFollowRequest = new PutFollowRequest(randomAlphaOfLength(4), randomAlphaOfLength(4), randomAlphaOfLength(4),
            randomBoolean() ? randomFrom(ActiveShardCount.NONE, ActiveShardCount.ONE, ActiveShardCount.DEFAULT, ActiveShardCount.ALL) : null
        );
        randomizeRequest(putFollowRequest);
        Request result = CcrRequestConverters.putFollow(putFollowRequest);
        assertThat(result.getMethod(), equalTo(HttpPut.METHOD_NAME));
        assertThat(result.getEndpoint(), equalTo("/" + putFollowRequest.getFollowerIndex() + "/_ccr/follow"));
        if (putFollowRequest.waitForActiveShards() != null && putFollowRequest.waitForActiveShards() != ActiveShardCount.DEFAULT) {
            String expectedValue = putFollowRequest.waitForActiveShards().toString().toLowerCase(Locale.ROOT);
            assertThat(result.getParameters().get("wait_for_active_shards"), equalTo(expectedValue));
        } else {
            assertThat(result.getParameters().size(), equalTo(0));
        }
        RequestConvertersTests.assertToXContentBody(putFollowRequest, result.getEntity());
    }

    public void testPauseFollow() {
        PauseFollowRequest pauseFollowRequest = new PauseFollowRequest(randomAlphaOfLength(4));
        Request result = CcrRequestConverters.pauseFollow(pauseFollowRequest);
        assertThat(result.getMethod(), equalTo(HttpPost.METHOD_NAME));
        assertThat(result.getEndpoint(), equalTo("/" + pauseFollowRequest.getFollowerIndex() + "/_ccr/pause_follow"));
        assertThat(result.getParameters().size(), equalTo(0));
        assertThat(result.getEntity(), nullValue());
    }

    public void testResumeFollow() throws Exception {
        ResumeFollowRequest resumeFollowRequest = new ResumeFollowRequest(randomAlphaOfLength(4));
        Request result = CcrRequestConverters.resumeFollow(resumeFollowRequest);
        assertThat(result.getMethod(), equalTo(HttpPost.METHOD_NAME));
        assertThat(result.getEndpoint(), equalTo("/" + resumeFollowRequest.getFollowerIndex() + "/_ccr/resume_follow"));
        assertThat(result.getParameters().size(), equalTo(0));
        RequestConvertersTests.assertToXContentBody(resumeFollowRequest, result.getEntity());
    }

    public void testUnfollow() {
        UnfollowRequest pauseFollowRequest = new UnfollowRequest(randomAlphaOfLength(4));
        Request result = CcrRequestConverters.unfollow(pauseFollowRequest);
        assertThat(result.getMethod(), equalTo(HttpPost.METHOD_NAME));
        assertThat(result.getEndpoint(), equalTo("/" + pauseFollowRequest.getFollowerIndex() + "/_ccr/unfollow"));
        assertThat(result.getParameters().size(), equalTo(0));
        assertThat(result.getEntity(), nullValue());
    }

    public void testForgetFollower() throws IOException {
        final ForgetFollowerRequest request = new ForgetFollowerRequest(
                randomAlphaOfLength(8),
                randomAlphaOfLength(8),
                randomAlphaOfLength(8),
                randomAlphaOfLength(8),
                randomAlphaOfLength(8));
        final Request convertedRequest = CcrRequestConverters.forgetFollower(request);
        assertThat(convertedRequest.getMethod(), equalTo(HttpPost.METHOD_NAME));
        assertThat(convertedRequest.getEndpoint(), equalTo("/" + request.leaderIndex() + "/_ccr/forget_follower"));
        assertThat(convertedRequest.getParameters().keySet(), empty());
        RequestConvertersTests.assertToXContentBody(request, convertedRequest.getEntity());
    }

    public void testPutAutofollowPattern() throws Exception {
        PutAutoFollowPatternRequest putAutoFollowPatternRequest = new PutAutoFollowPatternRequest(randomAlphaOfLength(4),
            randomAlphaOfLength(4), Arrays.asList(generateRandomStringArray(4, 4, false)));
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setFollowIndexNamePattern(randomAlphaOfLength(4));
        }
        randomizeRequest(putAutoFollowPatternRequest);

        Request result = CcrRequestConverters.putAutoFollowPattern(putAutoFollowPatternRequest);
        assertThat(result.getMethod(), equalTo(HttpPut.METHOD_NAME));
        assertThat(result.getEndpoint(), equalTo("/_ccr/auto_follow/" + putAutoFollowPatternRequest.getName()));
        assertThat(result.getParameters().size(), equalTo(0));
        RequestConvertersTests.assertToXContentBody(putAutoFollowPatternRequest, result.getEntity());
    }

    public void testDeleteAutofollowPattern() throws Exception {
        DeleteAutoFollowPatternRequest deleteAutoFollowPatternRequest = new DeleteAutoFollowPatternRequest(randomAlphaOfLength(4));

        Request result = CcrRequestConverters.deleteAutoFollowPattern(deleteAutoFollowPatternRequest);
        assertThat(result.getMethod(), equalTo(HttpDelete.METHOD_NAME));
        assertThat(result.getEndpoint(), equalTo("/_ccr/auto_follow/" + deleteAutoFollowPatternRequest.getName()));
        assertThat(result.getParameters().size(), equalTo(0));
        assertThat(result.getEntity(), nullValue());
    }

    public void testGetAutofollowPattern() throws Exception {
        GetAutoFollowPatternRequest deleteAutoFollowPatternRequest = new GetAutoFollowPatternRequest(randomAlphaOfLength(4));

        Request result = CcrRequestConverters.getAutoFollowPattern(deleteAutoFollowPatternRequest);
        assertThat(result.getMethod(), equalTo(HttpGet.METHOD_NAME));
        assertThat(result.getEndpoint(), equalTo("/_ccr/auto_follow/" + deleteAutoFollowPatternRequest.getName()));
        assertThat(result.getParameters().size(), equalTo(0));
        assertThat(result.getEntity(), nullValue());
    }

    public void testPauseAutofollowPattern() throws Exception {
        PauseAutoFollowPatternRequest pauseAutoFollowPatternRequest = new PauseAutoFollowPatternRequest(randomAlphaOfLength(4));

        Request result = CcrRequestConverters.pauseAutoFollowPattern(pauseAutoFollowPatternRequest);
        assertThat(result.getMethod(), equalTo(HttpPost.METHOD_NAME));
        assertThat(result.getEndpoint(), equalTo("/_ccr/auto_follow/" + pauseAutoFollowPatternRequest.getName() + "/pause"));
        assertThat(result.getParameters().size(), equalTo(0));
        assertThat(result.getEntity(), nullValue());
    }

    public void testResumeAutofollowPattern() throws Exception {
        ResumeAutoFollowPatternRequest resumeAutoFollowPatternRequest = new ResumeAutoFollowPatternRequest(randomAlphaOfLength(4));

        Request result = CcrRequestConverters.resumeAutoFollowPattern(resumeAutoFollowPatternRequest);
        assertThat(result.getMethod(), equalTo(HttpPost.METHOD_NAME));
        assertThat(result.getEndpoint(), equalTo("/_ccr/auto_follow/" + resumeAutoFollowPatternRequest.getName() + "/resume"));
        assertThat(result.getParameters().size(), equalTo(0));
        assertThat(result.getEntity(), nullValue());
    }

    public void testGetCcrStats() throws Exception {
        CcrStatsRequest ccrStatsRequest = new CcrStatsRequest();
        Request result = CcrRequestConverters.getCcrStats(ccrStatsRequest);
        assertThat(result.getMethod(), equalTo(HttpGet.METHOD_NAME));
        assertThat(result.getEndpoint(), equalTo("/_ccr/stats"));
        assertThat(result.getParameters().size(), equalTo(0));
        assertThat(result.getEntity(), nullValue());
    }

    public void testGetFollowStats() throws Exception {
        FollowStatsRequest followStatsRequest = new FollowStatsRequest(randomAlphaOfLength(4));
        Request result = CcrRequestConverters.getFollowStats(followStatsRequest);
        assertThat(result.getMethod(), equalTo(HttpGet.METHOD_NAME));
        assertThat(result.getEndpoint(), equalTo("/" + followStatsRequest.getFollowerIndex() + "/_ccr/stats"));
        assertThat(result.getParameters().size(), equalTo(0));
        assertThat(result.getEntity(), nullValue());
    }

    public void testGetFollowInfo() throws Exception {
        FollowInfoRequest followInfoRequest = new FollowInfoRequest(randomAlphaOfLength(4));
        Request result = CcrRequestConverters.getFollowInfo(followInfoRequest);
        assertThat(result.getMethod(), equalTo(HttpGet.METHOD_NAME));
        assertThat(result.getEndpoint(), equalTo("/" + followInfoRequest.getFollowerIndex() + "/_ccr/info"));
        assertThat(result.getParameters().size(), equalTo(0));
        assertThat(result.getEntity(), nullValue());
    }

    private static void randomizeRequest(FollowConfig request) {
        if (randomBoolean()) {
            request.setMaxOutstandingReadRequests(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxOutstandingWriteRequests(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxReadRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxReadRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            request.setMaxWriteBufferCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxWriteBufferSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            request.setMaxWriteRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxWriteRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            request.setMaxRetryDelay(new TimeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            request.setReadPollTimeout(new TimeValue(randomNonNegativeLong()));
        }
    }

}
