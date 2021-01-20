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

package org.elasticsearch.client.transform.transforms.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.client.transform.transforms.TimeRetentionPolicyConfig;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class TimeRetentionPolicyConfigTests extends AbstractResponseTestCase<
    org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig,
    TimeRetentionPolicyConfig> {

    public static org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig randomTimeSyncConfig() {
        return new org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig(
            randomAlphaOfLengthBetween(1, 10),
            new TimeValue(randomNonNegativeLong())
        );
    }

    public static void assertHlrcEquals(
        org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig serverTestInstance,
        TimeRetentionPolicyConfig clientInstance
    ) {
        assertEquals(serverTestInstance.getField(), clientInstance.getField());
        assertEquals(serverTestInstance.getMaxAge(), clientInstance.getMaxAge());
    }

    @Override
    protected org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig createServerTestInstance(
        XContentType xContentType
    ) {
        return randomTimeSyncConfig();
    }

    @Override
    protected TimeRetentionPolicyConfig doParseToClientInstance(XContentParser parser) throws IOException {
        return TimeRetentionPolicyConfig.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig serverTestInstance,
        TimeRetentionPolicyConfig clientInstance
    ) {
        assertHlrcEquals(serverTestInstance, clientInstance);
    }

}
