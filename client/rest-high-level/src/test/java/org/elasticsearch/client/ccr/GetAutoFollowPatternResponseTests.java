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

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.elasticsearch.client.ccr.PutAutoFollowPatternRequest.FOLLOW_PATTERN_FIELD;
import static org.elasticsearch.client.ccr.PutAutoFollowPatternRequest.LEADER_PATTERNS_FIELD;
import static org.elasticsearch.client.ccr.PutFollowRequest.REMOTE_CLUSTER_FIELD;
import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class GetAutoFollowPatternResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            this::createTestInstance,
            GetAutoFollowPatternResponseTests::toXContent,
            GetAutoFollowPatternResponse::fromXContent)
            .supportsUnknownFields(false)
            .test();
    }

    private GetAutoFollowPatternResponse createTestInstance() {
        int numPatterns = randomIntBetween(0, 16);
        NavigableMap<String, GetAutoFollowPatternResponse.Pattern> patterns = new TreeMap<>();
        for (int i = 0; i < numPatterns; i++) {
            GetAutoFollowPatternResponse.Pattern pattern = new GetAutoFollowPatternResponse.Pattern(
                randomAlphaOfLength(4), Collections.singletonList(randomAlphaOfLength(4)), randomAlphaOfLength(4));
            if (randomBoolean()) {
                pattern.setMaxOutstandingReadRequests(randomIntBetween(0, Integer.MAX_VALUE));
            }
            if (randomBoolean()) {
                pattern.setMaxOutstandingWriteRequests(randomIntBetween(0, Integer.MAX_VALUE));
            }
            if (randomBoolean()) {
                pattern.setMaxReadRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
            }
            if (randomBoolean()) {
                pattern.setMaxReadRequestSize(new ByteSizeValue(randomNonNegativeLong()));
            }
            if (randomBoolean()) {
                pattern.setMaxWriteBufferCount(randomIntBetween(0, Integer.MAX_VALUE));
            }
            if (randomBoolean()) {
                pattern.setMaxWriteBufferSize(new ByteSizeValue(randomNonNegativeLong()));
            }
            if (randomBoolean()) {
                pattern.setMaxWriteRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
            }
            if (randomBoolean()) {
                pattern.setMaxWriteRequestSize(new ByteSizeValue(randomNonNegativeLong()));
            }
            if (randomBoolean()) {
                pattern.setMaxRetryDelay(new TimeValue(randomNonNegativeLong()));
            }
            if (randomBoolean()) {
                pattern.setReadPollTimeout(new TimeValue(randomNonNegativeLong()));
            }
            patterns.put(randomAlphaOfLength(4), pattern);
        }
        return new GetAutoFollowPatternResponse(patterns);
    }

    public static void toXContent(GetAutoFollowPatternResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        {
            builder.startArray(GetAutoFollowPatternResponse.PATTERNS_FIELD.getPreferredName());
            for (Map.Entry<String, GetAutoFollowPatternResponse.Pattern> entry : response.getPatterns().entrySet()) {
                builder.startObject();
                {
                    builder.field(GetAutoFollowPatternResponse.NAME_FIELD.getPreferredName(), entry.getKey());
                    builder.startObject(GetAutoFollowPatternResponse.PATTERN_FIELD.getPreferredName());
                    {
                        GetAutoFollowPatternResponse.Pattern pattern = entry.getValue();
                        builder.field(REMOTE_CLUSTER_FIELD.getPreferredName(), pattern.getRemoteCluster());
                        builder.field(LEADER_PATTERNS_FIELD.getPreferredName(), pattern.getLeaderIndexPatterns());
                        if (pattern.getFollowIndexNamePattern()!= null) {
                            builder.field(FOLLOW_PATTERN_FIELD.getPreferredName(), pattern.getFollowIndexNamePattern());
                        }
                        entry.getValue().toXContentFragment(builder, ToXContent.EMPTY_PARAMS);
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();
    }
}
