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
package org.elasticsearch.cluster.service;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;

public class ClusterSerivceTests extends ESTestCase {

    public void testNewBuilderContainsCustoms() {
        ClusterState.Custom custom = new ClusterState.Custom() {
            @Override
            public Diff<ClusterState.Custom> diff(ClusterState.Custom previousState) {
                return null;
            }

            @Override
            public String getWriteableName() {
                return null;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {

            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return null;
            }
        };
        ClusterService service = new ClusterService(Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), null, Collections.singletonMap("foo", () ->
            custom));
        ClusterState.Builder builder = service.newClusterStateBuilder();
        assertSame(builder.build().custom("foo"), custom);
    }
}
