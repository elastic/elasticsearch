/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ccr;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction;
import org.elasticsearch.xpack.core.ccr.action.FollowParameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class FollowInfoResponseTests extends AbstractResponseTestCase<FollowInfoAction.Response, FollowInfoResponse> {

    @Override
    protected FollowInfoAction.Response createServerTestInstance(XContentType xContentType) {
        int numInfos = randomIntBetween(0, 32);
        List<FollowInfoAction.Response.FollowerInfo> infos = new ArrayList<>(numInfos);
        for (int i = 0; i < numInfos; i++) {
            FollowParameters followParameters = null;
            if (randomBoolean()) {
                followParameters = randomFollowParameters();
            }

            infos.add(new FollowInfoAction.Response.FollowerInfo(randomAlphaOfLength(4), randomAlphaOfLength(4), randomAlphaOfLength(4),
                randomFrom(FollowInfoAction.Response.Status.values()), followParameters));
        }
        return new FollowInfoAction.Response(infos);
    }

    static FollowParameters randomFollowParameters() {
        FollowParameters followParameters = new FollowParameters();
        followParameters.setMaxOutstandingReadRequests(randomIntBetween(0, Integer.MAX_VALUE));
        followParameters.setMaxOutstandingWriteRequests(randomIntBetween(0, Integer.MAX_VALUE));
        followParameters.setMaxReadRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        followParameters.setMaxWriteRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        followParameters.setMaxReadRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        followParameters.setMaxWriteRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        followParameters.setMaxWriteBufferCount(randomIntBetween(0, Integer.MAX_VALUE));
        followParameters.setMaxWriteBufferSize(new ByteSizeValue(randomNonNegativeLong()));
        followParameters.setMaxRetryDelay(new TimeValue(randomNonNegativeLong()));
        followParameters.setReadPollTimeout(new TimeValue(randomNonNegativeLong()));
        return followParameters;
    }

    @Override
    protected FollowInfoResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return FollowInfoResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(FollowInfoAction.Response serverTestInstance, FollowInfoResponse clientInstance) {
        assertThat(serverTestInstance.getFollowInfos().size(), equalTo(clientInstance.getInfos().size()));
        for (int i = 0; i < serverTestInstance.getFollowInfos().size(); i++) {
            FollowInfoAction.Response.FollowerInfo serverFollowInfo = serverTestInstance.getFollowInfos().get(i);
            FollowInfoResponse.FollowerInfo clientFollowerInfo = clientInstance.getInfos().get(i);

            assertThat(serverFollowInfo.getRemoteCluster(), equalTo(clientFollowerInfo.getRemoteCluster()));
            assertThat(serverFollowInfo.getLeaderIndex(), equalTo(clientFollowerInfo.getLeaderIndex()));
            assertThat(serverFollowInfo.getFollowerIndex(), equalTo(clientFollowerInfo.getFollowerIndex()));
            assertThat(serverFollowInfo.getStatus().toString().toLowerCase(Locale.ROOT),
                equalTo(clientFollowerInfo.getStatus().getName().toLowerCase(Locale.ROOT)));

            FollowParameters serverParams = serverFollowInfo.getParameters();
            FollowConfig clientParams = clientFollowerInfo.getParameters();
            if (serverParams != null) {
                assertThat(serverParams.getMaxReadRequestOperationCount(), equalTo(clientParams.getMaxReadRequestOperationCount()));
                assertThat(serverParams.getMaxWriteRequestOperationCount(), equalTo(clientParams.getMaxWriteRequestOperationCount()));
                assertThat(serverParams.getMaxOutstandingReadRequests(), equalTo(clientParams.getMaxOutstandingReadRequests()));
                assertThat(serverParams.getMaxOutstandingWriteRequests(), equalTo(clientParams.getMaxOutstandingWriteRequests()));
                assertThat(serverParams.getMaxReadRequestSize(), equalTo(clientParams.getMaxReadRequestSize()));
                assertThat(serverParams.getMaxWriteRequestSize(), equalTo(clientParams.getMaxWriteRequestSize()));
                assertThat(serverParams.getMaxWriteBufferCount(), equalTo(clientParams.getMaxWriteBufferCount()));
                assertThat(serverParams.getMaxWriteBufferSize(), equalTo(clientParams.getMaxWriteBufferSize()));
                assertThat(serverParams.getMaxRetryDelay(), equalTo(clientParams.getMaxRetryDelay()));
                assertThat(serverParams.getReadPollTimeout(), equalTo(clientParams.getReadPollTimeout()));
            } else {
                assertThat(clientParams, nullValue());
            }
        }
    }

}
