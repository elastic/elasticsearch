/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.XContentTestUtils;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class ClusterStateSerializationTests extends ESTestCase {

    public void testSerializationInCurrentVersion() throws IOException {
        assertSerializationRoundTrip(TransportVersion.current());
    }

    public void testSerializationPreMultiProject() throws IOException {
        assertSerializationRoundTrip(TransportVersionUtils.getPreviousVersion(TransportVersions.MULTI_PROJECT));
    }

    private void assertSerializationRoundTrip(TransportVersion transportVersion) throws IOException {
        ClusterState original = randomClusterState(transportVersion);
        DiscoveryNode node = original.nodes().getLocalNode();
        assertThat(node, Matchers.notNullValue());

        final ClusterState deserialized = ESTestCase.copyWriteable(
            original,
            new NamedWriteableRegistry(ClusterModule.getNamedWriteables()),
            in -> ClusterState.readFrom(in, node),
            transportVersion
        );
        assertEquivalent("For transport version: " + transportVersion, original, deserialized);
    }

    private void assertEquivalent(String context, ClusterState expected, ClusterState actual) throws IOException {
        if (expected == actual) {
            return;
        }
        // The simplest model we have for comparing equivalence is by comparing the XContent of the cluster state
        var expectedJson = XContentTestUtils.convertToMap(expected);
        var actualJson = XContentTestUtils.convertToMap(actual);
        assertThat(context, actualJson, MapMatcher.matchesMap(expectedJson));
    }

    private ClusterState randomClusterState(TransportVersion transportVersion) {
        final Set<String> datastreamNames = randomSet(0, 10, () -> randomAlphaOfLengthBetween(4, 18));
        final List<Tuple<String, Integer>> datastreams = datastreamNames.stream()
            .map(name -> new Tuple<>(name, randomIntBetween(1, 5)))
            .toList();
        final List<String> indices = List.copyOf(
            randomSet(0, 10, () -> randomValueOtherThanMany(datastreamNames::contains, () -> randomAlphaOfLengthBetween(3, 12)))
        );

        final DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        do {
            final String id = randomUUID();
            nodes.add(DiscoveryNodeUtils.create(id));
            nodes.localNodeId(id);
        } while (randomBoolean());

        ProjectMetadata project = DataStreamTestHelper.getProjectWithDataStreams(datastreams, indices);
        return ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(project)).nodes(nodes).build();
    }
}
