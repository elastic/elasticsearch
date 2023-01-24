/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.EqualsHashCodeTestUtils.CopyFunction;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class CoordinationMetadataTests extends ESTestCase {

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
        // Note: the explicit cast of the CopyFunction is needed for some IDE (specifically Eclipse 4.8.0) to infer the right type
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            initialConfig,
            (CopyFunction<VotingConfiguration>) orig -> ESTestCase.copyWriteable(
                orig,
                new NamedWriteableRegistry(Collections.emptyList()),
                VotingConfiguration::new
            ),
            cfg -> randomlyChangeVotingConfiguration(cfg)
        );
    }

    private static VotingConfiguration randomVotingConfig() {
        return new VotingConfiguration(Sets.newHashSet(generateRandomStringArray(randomInt(10), 20, false)));
    }

    public void testVotingTombstoneSerializationEqualsHashCode() {
        VotingConfigExclusion tombstone = new VotingConfigExclusion(randomAlphaOfLength(10), randomAlphaOfLength(10));
        // Note: the explicit cast of the CopyFunction is needed for some IDE (specifically Eclipse 4.8.0) to infer the right type
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            tombstone,
            (CopyFunction<VotingConfigExclusion>) orig -> ESTestCase.copyWriteable(
                orig,
                new NamedWriteableRegistry(Collections.emptyList()),
                VotingConfigExclusion::new
            ),
            orig -> randomlyChangeVotingTombstone(orig)
        );
    }

    public void testVotingTombstoneXContent() throws IOException {
        VotingConfigExclusion originalTombstone = new VotingConfigExclusion(randomAlphaOfLength(10), randomAlphaOfLength(10));

        final XContentBuilder builder = JsonXContent.contentBuilder();
        originalTombstone.toXContent(builder, ToXContent.EMPTY_PARAMS);

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final VotingConfigExclusion fromXContentTombstone = VotingConfigExclusion.fromXContent(parser);
            assertThat(originalTombstone, equalTo(fromXContentTombstone));
        }
    }

    private VotingConfigExclusion randomlyChangeVotingTombstone(VotingConfigExclusion tombstone) {
        if (randomBoolean()) {
            return new VotingConfigExclusion(randomAlphaOfLength(10), tombstone.getNodeName());
        } else {
            return new VotingConfigExclusion(tombstone.getNodeId(), randomAlphaOfLength(10));
        }
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

    private Set<VotingConfigExclusion> randomVotingTombstones() {
        final int size = randomIntBetween(1, 10);
        final Set<VotingConfigExclusion> nodes = Sets.newHashSetWithExpectedSize(size);
        while (nodes.size() < size) {
            assertTrue(nodes.add(new VotingConfigExclusion(randomAlphaOfLength(10), randomAlphaOfLength(10))));
        }
        return nodes;
    }

    public void testCoordinationMetadataSerializationEqualsHashCode() {
        CoordinationMetadata initialMetadata = new CoordinationMetadata(
            randomNonNegativeLong(),
            randomVotingConfig(),
            randomVotingConfig(),
            randomVotingTombstones()
        );
        // Note: the explicit cast of the CopyFunction is needed for some IDE (specifically Eclipse 4.8.0) to infer the right type
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            initialMetadata,
            (CopyFunction<CoordinationMetadata>) orig -> ESTestCase.copyWriteable(
                orig,
                new NamedWriteableRegistry(Collections.emptyList()),
                CoordinationMetadata::new
            ),
            meta -> {
                CoordinationMetadata.Builder builder = CoordinationMetadata.builder(meta);
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
                        if (meta.getVotingConfigExclusions().isEmpty() == false && randomBoolean()) {
                            builder.clearVotingConfigExclusions();
                        } else {
                            randomVotingTombstones().forEach(dn -> builder.addVotingConfigExclusion(dn));
                        }
                        break;
                }
                return builder.build();
            }
        );
    }

    public void testXContent() throws IOException {
        CoordinationMetadata originalMeta = new CoordinationMetadata(
            randomNonNegativeLong(),
            randomVotingConfig(),
            randomVotingConfig(),
            randomVotingTombstones()
        );

        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        originalMeta.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final CoordinationMetadata fromXContentMeta = CoordinationMetadata.fromXContent(parser);
            assertThat(originalMeta, equalTo(fromXContentMeta));
        }
    }
}
