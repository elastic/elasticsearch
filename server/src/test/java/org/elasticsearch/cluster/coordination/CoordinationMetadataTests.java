/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
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

    private static VotingConfiguration randomVotingConfig() {
        return new VotingConfiguration(Sets.newHashSet(generateRandomStringArray(randomInt(10), 20, false)));
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

    private Set<VotingConfigExclusion> randomVotingTombstones() {
        final int size = randomIntBetween(1, 10);
        final Set<VotingConfigExclusion> nodes = Sets.newHashSetWithExpectedSize(size);
        while (nodes.size() < size) {
            assertTrue(nodes.add(new VotingConfigExclusion(randomAlphaOfLength(10), randomAlphaOfLength(10))));
        }
        return nodes;
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

    /**
     * Verifies that {@link CoordinationMetadata#EMPTY_METADATA} has default term zero and empty voting configs and exclusions.
     */
    public void testCoordinationMetadataEmptyMetadata() {
        CoordinationMetadata emptyMetadata = CoordinationMetadata.EMPTY_METADATA;
        assertThat(emptyMetadata.term(), equalTo(0L));
        assertThat(emptyMetadata.getLastCommittedConfiguration(), equalTo(VotingConfiguration.EMPTY_CONFIG));
        assertThat(emptyMetadata.getLastAcceptedConfiguration(), equalTo(VotingConfiguration.EMPTY_CONFIG));
        assertThat(emptyMetadata.getVotingConfigExclusions(), equalTo(Set.of()));
    }

    /**
     * Verifies that {@link CoordinationMetadata.Builder} builds metadata whose getters match the values set on the builder.
     */
    public void testCoordinationMetadataBuilder() {
        long term = randomNonNegativeLong();
        VotingConfiguration lastCommittedConfiguration = randomVotingConfig();
        VotingConfiguration lastAcceptedConfiguration = randomVotingConfig();
        Set<VotingConfigExclusion> votingConfigExclusions = randomVotingTombstones();
        CoordinationMetadata.Builder builder = CoordinationMetadata.builder()
            .term(term)
            .lastCommittedConfiguration(lastCommittedConfiguration)
            .lastAcceptedConfiguration(lastAcceptedConfiguration);
        for (VotingConfigExclusion votingConfigExclusion : votingConfigExclusions) {
            builder.addVotingConfigExclusion(votingConfigExclusion);
        }
        CoordinationMetadata coordinationMetadata = builder.build();
        assertThat(coordinationMetadata.term(), equalTo(term));
        assertThat(coordinationMetadata.getLastCommittedConfiguration(), equalTo(lastCommittedConfiguration));
        assertThat(coordinationMetadata.getLastAcceptedConfiguration(), equalTo(lastAcceptedConfiguration));
        assertThat(coordinationMetadata.getVotingConfigExclusions(), equalTo(votingConfigExclusions));
    }

    /**
     * Verifies that {@link VotingConfiguration#MUST_JOIN_ELECTED_MASTER} has the expected single node id and that
     * {@link VotingConfiguration#hasQuorum} returns true only when the supplied set contains that id.
     */
    public void testVotingConfigurationMustJoinElectedMaster() {
        VotingConfiguration mustJoin = VotingConfiguration.MUST_JOIN_ELECTED_MASTER;
        assertThat(mustJoin.isEmpty(), equalTo(false));
        assertThat(mustJoin.getNodeIds(), equalTo(Set.of("_must_join_elected_master_")));
        assertThat(mustJoin.hasQuorum(Set.of("_must_join_elected_master_")), equalTo(true));
        assertThat(mustJoin.hasQuorum(Set.of("_must_join_elected_master_", "other")), equalTo(true));
        assertThat(mustJoin.hasQuorum(Set.of("other")), equalTo(false));
        assertThat(mustJoin.hasQuorum(Set.of()), equalTo(false));
    }

    /**
     * Verifies that {@link VotingConfigExclusion} getters return the values passed to the constructor.
     */
    public void testVotingConfigExclusionGetters() {
        String nodeId = randomAlphaOfLength(10);
        String nodeName = randomAlphaOfLength(10);
        VotingConfigExclusion votingConfigExclusion = new VotingConfigExclusion(nodeId, nodeName);
        assertThat(votingConfigExclusion.getNodeId(), equalTo(nodeId));
        assertThat(votingConfigExclusion.getNodeName(), equalTo(nodeName));
    }

    /**
     * Verifies that {@link VotingConfigExclusion#MISSING_VALUE_MARKER} has the expected value used when a node id or
     * name is absent (e.g. when excluding a node known only by name before it has joined). Prevents accidental change.
     */
    public void testVotingConfigExclusionMissingValueMarker() {
        assertThat(VotingConfigExclusion.MISSING_VALUE_MARKER, equalTo("_absent_"));
    }
}
