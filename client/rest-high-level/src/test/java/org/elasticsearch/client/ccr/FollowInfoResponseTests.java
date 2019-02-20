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

package org.elasticsearch.client.ccr;

import org.elasticsearch.client.ccr.FollowInfoResponse.FollowerInfo;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class FollowInfoResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            FollowInfoResponseTests::createTestInstance,
            FollowInfoResponseTests::toXContent,
            FollowInfoResponse::fromXContent)
            .supportsUnknownFields(true)
            .test();
    }

    private static void toXContent(FollowInfoResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.startArray(FollowInfoResponse.FOLLOWER_INDICES_FIELD.getPreferredName());
        for (FollowerInfo info : response.getInfos()) {
            builder.startObject();
            builder.field(FollowerInfo.FOLLOWER_INDEX_FIELD.getPreferredName(), info.getFollowerIndex());
            builder.field(FollowerInfo.REMOTE_CLUSTER_FIELD.getPreferredName(), info.getRemoteCluster());
            builder.field(FollowerInfo.LEADER_INDEX_FIELD.getPreferredName(), info.getLeaderIndex());
            builder.field(FollowerInfo.STATUS_FIELD.getPreferredName(), info.getStatus().getName());
            if (info.getParameters() != null) {
                builder.startObject(FollowerInfo.PARAMETERS_FIELD.getPreferredName());
                {
                    info.getParameters().toXContentFragment(builder, ToXContent.EMPTY_PARAMS);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
    }

    private static FollowInfoResponse createTestInstance() {
        int numInfos = randomIntBetween(0, 64);
        List<FollowerInfo> infos = new ArrayList<>(numInfos);
        for (int i = 0; i < numInfos; i++) {
            FollowInfoResponse.Status status = randomFrom(FollowInfoResponse.Status.values());
            FollowConfig followConfig = randomBoolean() ? FollowConfigTests.createTestInstance() : null;
            infos.add(new FollowerInfo(randomAlphaOfLength(4), randomAlphaOfLength(4), randomAlphaOfLength(4), status, followConfig));
        }
        return new FollowInfoResponse(infos);
    }

}
