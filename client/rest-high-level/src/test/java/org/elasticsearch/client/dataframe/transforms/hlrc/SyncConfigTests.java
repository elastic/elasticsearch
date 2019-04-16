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

package org.elasticsearch.client.dataframe.transforms.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.client.dataframe.transforms.SyncConfig;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class SyncConfigTests
        extends AbstractResponseTestCase<org.elasticsearch.xpack.core.dataframe.transforms.SyncConfig, SyncConfig> {

    public static org.elasticsearch.xpack.core.dataframe.transforms.SyncConfig randomSyncConfig() {
        return new org.elasticsearch.xpack.core.dataframe.transforms.SyncConfig(TimeSyncConfigTests.randomTimeSyncConfig());
    }

    @Override
    protected org.elasticsearch.xpack.core.dataframe.transforms.SyncConfig createServerTestInstance() {
        return randomSyncConfig();
    }

    @Override
    protected SyncConfig doParseToClientInstance(XContentParser parser) throws IOException {
        return SyncConfig.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.xpack.core.dataframe.transforms.SyncConfig serverTestInstance,
            SyncConfig clientInstance) {

        if (serverTestInstance.getTimeSyncConfig() != null) {
            assertNotNull(clientInstance.getTimeSyncConfig());
            TimeSyncConfigTests.assertHlrcEquals(serverTestInstance.getTimeSyncConfig(), clientInstance.getTimeSyncConfig());
        } else {
            assertNull(clientInstance.getTimeSyncConfig());
        }
    }

}
