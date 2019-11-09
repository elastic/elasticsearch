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
package org.elasticsearch.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.cluster.coordination.CoordinationMetaData;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.StreamsUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.apache.http.client.methods.RequestBuilder.put;
import static org.elasticsearch.Version.V_8_0_0;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ClusterStateTests extends ESTestCase {

    public void testSupersedes() {
        final Version version = Version.CURRENT;
        final DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), version);
        final DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), version);
        final DiscoveryNodes nodes = DiscoveryNodes.builder().add(node1).add(node2).build();
        ClusterName name = ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY);
        ClusterState noMaster1 = ClusterState.builder(name).version(randomInt(5)).nodes(nodes).build();
        ClusterState noMaster2 = ClusterState.builder(name).version(randomInt(5)).nodes(nodes).build();
        ClusterState withMaster1a = ClusterState.builder(name).version(randomInt(5)).nodes(DiscoveryNodes.builder(nodes)
            .masterNodeId(node1.getId())).build();
        ClusterState withMaster1b = ClusterState.builder(name).version(randomInt(5)).nodes(DiscoveryNodes.builder(nodes)
            .masterNodeId(node1.getId())).build();
        ClusterState withMaster2 = ClusterState.builder(name).version(randomInt(5)).nodes(DiscoveryNodes.builder(nodes)
            .masterNodeId(node2.getId())).build();

        // states with no master should never supersede anything
        assertFalse(noMaster1.supersedes(noMaster2));
        assertFalse(noMaster1.supersedes(withMaster1a));

        // states should never supersede states from another master
        assertFalse(withMaster1a.supersedes(withMaster2));
        assertFalse(withMaster1a.supersedes(noMaster1));

        // state from the same master compare by version
        assertThat(withMaster1a.supersedes(withMaster1b), equalTo(withMaster1a.version() > withMaster1b.version()));
    }

    public void testBuilderRejectsNullCustom() {
        final ClusterState.Builder builder = ClusterState.builder(ClusterName.DEFAULT);
        final String key = randomAlphaOfLength(10);
        assertThat(expectThrows(NullPointerException.class, () -> builder.putCustom(key, null)).getMessage(), containsString(key));
    }

    public void testBuilderRejectsNullInCustoms() {
        final ClusterState.Builder builder = ClusterState.builder(ClusterName.DEFAULT);
        final String key = randomAlphaOfLength(10);
        final ImmutableOpenMap.Builder<String, ClusterState.Custom> mapBuilder = ImmutableOpenMap.builder();
        mapBuilder.put(key, null);
        final ImmutableOpenMap<String, ClusterState.Custom> map = mapBuilder.build();
        assertThat(expectThrows(NullPointerException.class, () -> builder.customs(map)).getMessage(), containsString(key));
    }

    public void testToXContentShouldDelegateToMembers() throws IOException {
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                                                        .stateUUID("stateUUID")
                                                        .metaData(MetaData.builder()
                                                            .clusterUUID("clusterUUID")
                                                            .coordinationMetaData(CoordinationMetaData.builder()
                                                                .build())
                                                            .put(IndexMetaData.builder("index1")
                                                                .state(IndexMetaData.State.OPEN)
                                                                .settings(Settings.builder()
                                                                    .put(SETTING_VERSION_CREATED, V_8_0_0))
                                                                .putMapping(new MappingMetaData("type1",
                                                                    new HashMap<>(){{
                                                                        put("key11", new HashMap<String, Object>());
                                                                        put("key12", new HashMap<String, Object>());
                                                                        put("key13", new HashMap<String, Object>());
                                                                    }}))
                                                                .putAlias(AliasMetaData.builder("alias11")
                                                                    .indexRouting("indexRouting11")
                                                                    .build())
                                                                .putAlias(AliasMetaData.builder("alias12")
                                                                    .indexRouting("indexRouting12")
                                                                    .build())
                                                                .numberOfShards(2)
                                                                .primaryTerm(0, 11L)
                                                                .primaryTerm(1, 12L)
                                                                .putInSyncAllocationIds(0, new HashSet<>(){{
                                                                    put("allocationId11");
                                                                    put("allocationId12");
                                                                }})
                                                                .putInSyncAllocationIds(1, new HashSet<>(){{
                                                                    put("allocationId13");
                                                                    put("allocationId14");
                                                                }})
                                                                .numberOfReplicas(2)
                                                                .putRolloverInfo(new RolloverInfo("rolloveAlias1", new ArrayList<>(), 1L)))
                                                            .put(IndexMetaData.builder("index2")
                                                                .state(IndexMetaData.State.OPEN)
                                                                .settings(Settings.builder()
                                                                    .put(SETTING_VERSION_CREATED, V_8_0_0))
                                                                .putMapping(new MappingMetaData("type2",
                                                                    // the type name is the root value,
                                                                    // the original logic in ClusterState.toXContent will reduce
                                                                    new HashMap<>(){{
                                                                        put("type2", new HashMap<String, Object>());
                                                                    }}))
                                                                .numberOfShards(3)
                                                                .primaryTerm(0, 21L)
                                                                .primaryTerm(1, 22L)
                                                                .primaryTerm(2, 23L)
                                                                .putInSyncAllocationIds(0, new HashSet<>(){{
                                                                    put("allocationId21");
                                                                }})
                                                                .putInSyncAllocationIds(1, new HashSet<>(){{
                                                                    put("allocationId23");
                                                                    put("allocationId24");
                                                                }})
                                                                .putInSyncAllocationIds(2, new HashSet<>(){{
                                                                    put("allocationId25");
                                                                    put("allocationId26");
                                                                    put("allocationId27");
                                                                }})
                                                                .numberOfReplicas(3)
                                                                .putRolloverInfo(new RolloverInfo("rolloveAlias2", new ArrayList<>(), 1L))))
                                                        .build();

        ToXContent.Params params = new ToXContent.MapParams(new HashMap(){{

        }});

        ObjectMapper mapper = new ObjectMapper();
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            clusterState.toXContent(builder, params);
            builder.endObject();

            String expected = StreamsUtils.copyToStringFromClasspath("/org/elasticsearch/cluster/cluster-state-toxcontent.json");

            assertEquals(mapper.readTree(expected), mapper.readTree(Strings.toString(builder)));
        }

    }
}
