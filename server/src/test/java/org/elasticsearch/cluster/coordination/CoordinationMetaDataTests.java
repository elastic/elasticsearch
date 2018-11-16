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
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfiguration;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.equalTo;

public class CoordinationMetaDataTests extends ESTestCase {

    public void testVotingConfiguration() {
        VotingConfiguration config0 = new VotingConfiguration(Sets.newHashSet());
        assertThat(config0, equalTo(VotingConfiguration.EMPTY_CONFIG));
        assertThat(config0.getNodeIds(), equalTo(Sets.newHashSet()));
        assertThat(config0.isEmpty(), equalTo(true));
        assertThat(config0.hasQuorum(Sets.newHashSet()), equalTo(false));
        assertThat(config0.hasQuorum(Sets.newHashSet("id1")), equalTo(false));

        VotingConfiguration config1 = new VotingConfiguration(Sets.newHashSet("id1"));
        assertThat(config1.getNodeIds(), equalTo(Sets.newHashSet("id1")));
        assertThat(config1.isEmpty(), equalTo(false));
        assertThat(config1.hasQuorum(Sets.newHashSet("id1")), equalTo(true));
        assertThat(config1.hasQuorum(Sets.newHashSet("id1", "id2")), equalTo(true));
        assertThat(config1.hasQuorum(Sets.newHashSet("id2")), equalTo(false));
        assertThat(config1.hasQuorum(Sets.newHashSet()), equalTo(false));

        VotingConfiguration config2 = new VotingConfiguration(Sets.newHashSet("id1", "id2"));
        assertThat(config2.getNodeIds(), equalTo(Sets.newHashSet("id1", "id2")));
        assertThat(config2.isEmpty(), equalTo(false));
        assertThat(config2.hasQuorum(Sets.newHashSet("id1", "id2")), equalTo(true));
        assertThat(config2.hasQuorum(Sets.newHashSet("id1", "id2", "id3")), equalTo(true));
        assertThat(config2.hasQuorum(Sets.newHashSet("id1")), equalTo(false));
        assertThat(config2.hasQuorum(Sets.newHashSet("id2")), equalTo(false));
        assertThat(config2.hasQuorum(Sets.newHashSet("id3")), equalTo(false));
        assertThat(config2.hasQuorum(Sets.newHashSet("id1", "id3")), equalTo(false));
        assertThat(config2.hasQuorum(Sets.newHashSet()), equalTo(false));

        VotingConfiguration config3 = new VotingConfiguration(Sets.newHashSet("id1", "id2", "id3"));
        assertThat(config3.getNodeIds(), equalTo(Sets.newHashSet("id1", "id2", "id3")));
        assertThat(config3.isEmpty(), equalTo(false));
        assertThat(config3.hasQuorum(Sets.newHashSet("id1", "id2")), equalTo(true));
        assertThat(config3.hasQuorum(Sets.newHashSet("id2", "id3")), equalTo(true));
        assertThat(config3.hasQuorum(Sets.newHashSet("id1", "id3")), equalTo(true));
        assertThat(config3.hasQuorum(Sets.newHashSet("id1", "id2", "id3")), equalTo(true));
        assertThat(config3.hasQuorum(Sets.newHashSet("id1", "id2", "id4")), equalTo(true));
        assertThat(config3.hasQuorum(Sets.newHashSet("id1")), equalTo(false));
        assertThat(config3.hasQuorum(Sets.newHashSet("id2")), equalTo(false));
        assertThat(config3.hasQuorum(Sets.newHashSet("id3")), equalTo(false));
        assertThat(config3.hasQuorum(Sets.newHashSet("id1", "id4")), equalTo(false));
        assertThat(config3.hasQuorum(Sets.newHashSet("id1", "id4", "id5")), equalTo(false));
        assertThat(config3.hasQuorum(Sets.newHashSet()), equalTo(false));
    }

    public void testVotingConfigurationSerializationEqualsHashCode() {
        VotingConfiguration initialConfig = randomVotingConfig();
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(initialConfig,
            orig -> ESTestCase.copyWriteable(orig, new NamedWriteableRegistry(Collections.emptyList()), VotingConfiguration::new),
            cfg -> randomlyChangeVotingConfiguration(cfg));
    }

    private static VotingConfiguration randomVotingConfig() {
        return new VotingConfiguration(Sets.newHashSet(generateRandomStringArray(randomInt(10), 20, false)));
    }

    private VotingConfiguration randomlyChangeVotingConfiguration(VotingConfiguration cfg) {
        Set<String> newNodeIds = new HashSet<>(cfg.getNodeIds());
        if (cfg.isEmpty() == false && randomBoolean()) {
            // remove random element
            newNodeIds.remove(randomFrom(cfg.getNodeIds()));
        } else if (cfg.isEmpty() == false && randomBoolean()) {
            // change random element
            newNodeIds.remove(randomFrom(cfg.getNodeIds()));
            newNodeIds.add(randomAlphaOfLength(20));
        } else {
            // add random element
            newNodeIds.add(randomAlphaOfLength(20));
        }
        return new VotingConfiguration(newNodeIds);
    }

    private Set<DiscoveryNode> randomDiscoveryNodeSet() {
        final int size = randomIntBetween(1, 10);
        final Set<DiscoveryNode> nodes = new HashSet<>(size);
        while (nodes.size() < size) {
            assertTrue(nodes.add(new DiscoveryNode(randomAlphaOfLength(10), randomAlphaOfLength(10),
                UUIDs.randomBase64UUID(random()), randomAlphaOfLength(10), randomAlphaOfLength(10), buildNewFakeTransportAddress(),
                emptyMap(), singleton(DiscoveryNode.Role.MASTER), Version.CURRENT)));
        }
        return nodes;
    }

    public void testCoordinationMetaDataSerializationEqualsHashCode() {
        CoordinationMetaData initialMetaData = new CoordinationMetaData(randomNonNegativeLong(), randomVotingConfig(), randomVotingConfig(),
                randomDiscoveryNodeSet());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(initialMetaData,
            orig -> ESTestCase.copyWriteable(orig, new NamedWriteableRegistry(Collections.emptyList()), CoordinationMetaData::new),
            meta -> {
                CoordinationMetaData.Builder builder = CoordinationMetaData.builder(meta);
                switch (randomInt(3)) {
                    case 0:
                        builder.term(randomValueOtherThan(meta.term(), ESTestCase::randomNonNegativeLong));
                        break;
                    case 1:
                        builder.lastCommittedConfiguration(randomlyChangeVotingConfiguration(meta.getLastCommittedConfiguration()));
                        break;
                    case 2:
                        builder.lastAcceptedConfiguration(randomlyChangeVotingConfiguration(meta.getLastAcceptedConfiguration()));
                        break;
                    case 3:
                        if (meta.getVotingTombstones().isEmpty() == false && randomBoolean()) {
                            builder.clearVotingTombstones();
                        } else {
                            randomDiscoveryNodeSet().forEach(dn -> builder.addVotingTombstone(dn));
                        }
                        break;
                }
                return builder.build();
            });
    }

    public void testXContent() throws IOException {
        CoordinationMetaData originalMeta = new CoordinationMetaData(randomNonNegativeLong(), randomVotingConfig(), randomVotingConfig(),
                Collections.emptySet()); //TODO use non-empty tombstones set once toXContent for tombstones is implemented

        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        originalMeta.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final CoordinationMetaData fromXContentMeta = CoordinationMetaData.fromXContent(parser);
            assertThat(originalMeta, equalTo(fromXContentMeta));
        }
    }
}
