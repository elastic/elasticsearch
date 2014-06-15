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

package org.elasticsearch.discovery.gce;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Ignore;

/**
 * We need to ignore this test from elasticsearch version 1.2.1 as
 * expected nodes running is 2 and this test will create 2 clusters with one node each.
 * @see org.elasticsearch.test.ElasticsearchIntegrationTest#ensureClusterSizeConsistency()
 * TODO Reactivate when it will be possible to set the number of running nodes
 */
@Ignore
public class GceDifferentTagsTwoTagsTest extends AbstractGceComputeServiceTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder().put(super.nodeSettings(nodeOrdinal));
        settings.put("discovery.type", "gce");
        settings.put("cloud.gce.api.impl", GceComputeServiceTwoNodesDifferentTagsMock.class);
        settings.put("discovery.gce.tags", "elasticsearch,dev");
        return settings.build();
    }

    /**
     * Set the number of expected nodes in the current cluster
     */
    @Override
    protected int getExpectedNodes() {
        return 1;
    }
}
