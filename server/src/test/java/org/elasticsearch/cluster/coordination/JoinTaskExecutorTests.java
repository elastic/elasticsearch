/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.HashSet;
import java.util.List;

import static org.elasticsearch.test.VersionUtils.maxCompatibleVersion;
import static org.elasticsearch.test.VersionUtils.randomCompatibleVersion;
import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.elasticsearch.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JoinTaskExecutorTests extends ESTestCase {

    public void testPreventJoinClusterWithNewerIndices() {
        Settings.builder().build();
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
        JoinTaskExecutor.ensureIndexCompatibility(Version.CURRENT, metadata);

        expectThrows(
            IllegalStateException.class,
            () -> JoinTaskExecutor.ensureIndexCompatibility(VersionUtils.getPreviousVersion(Version.CURRENT), metadata)
        );
    }

    public void testPreventJoinClusterWithUnsupportedIndices() {
        Settings.builder().build();
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.fromString("6.8.0"))) // latest V6 released version
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
        expectThrows(IllegalStateException.class, () -> JoinTaskExecutor.ensureIndexCompatibility(Version.CURRENT, metadata));
    }

    public void testPreventJoinClusterWithUnsupportedNodeVersions() {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        final Version version = randomCompatibleVersion(random(), Version.CURRENT);
        builder.add(new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), version));
        builder.add(new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), randomCompatibleVersion(random(), version)));
        DiscoveryNodes nodes = builder.build();

        final Version maxNodeVersion = nodes.getMaxNodeVersion();
        final Version minNodeVersion = nodes.getMinNodeVersion();

        final Version tooLow = Version.fromId(maxNodeVersion.minimumCompatibilityVersion().id - 100);
        expectThrows(IllegalStateException.class, () -> {
            if (randomBoolean()) {
                JoinTaskExecutor.ensureNodesCompatibility(tooLow, nodes);
            } else {
                JoinTaskExecutor.ensureNodesCompatibility(tooLow, minNodeVersion, maxNodeVersion);
            }
        });

        final Version oldVersion = randomValueOtherThanMany(
            v -> v.onOrAfter(minNodeVersion),
            () -> rarely() ? Version.fromId(minNodeVersion.id - 1) : randomVersion(random())
        );
        expectThrows(IllegalStateException.class, () -> JoinTaskExecutor.ensureVersionBarrier(oldVersion, minNodeVersion));

        final Version minGoodVersion = maxNodeVersion.major == minNodeVersion.major ?
        // we have to stick with the same major
            minNodeVersion : maxNodeVersion.minimumCompatibilityVersion();
        final Version justGood = randomVersionBetween(random(), minGoodVersion, maxCompatibleVersion(minNodeVersion));

        if (randomBoolean()) {
            JoinTaskExecutor.ensureNodesCompatibility(justGood, nodes);
        } else {
            JoinTaskExecutor.ensureNodesCompatibility(justGood, minNodeVersion, maxNodeVersion);
        }
    }

    public void testSuccess() {
        Settings.builder().build();
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(randomCompatibleVersionSettings())
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        metaBuilder.put(indexMetadata, false);
        indexMetadata = IndexMetadata.builder("test1")
            .settings(randomCompatibleVersionSettings())
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
        JoinTaskExecutor.ensureIndexCompatibility(Version.CURRENT, metadata);
    }

    public static Settings.Builder randomCompatibleVersionSettings() {
        Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            builder.put(IndexMetadata.SETTING_VERSION_CREATED, getRandomCompatibleVersion());
            if (randomBoolean()) {
                builder.put(IndexMetadata.SETTING_VERSION_COMPATIBILITY, getRandomCompatibleVersion());
            }
        } else {
            builder.put(IndexMetadata.SETTING_VERSION_CREATED, randomFrom(Version.fromString("5.0.0"), Version.fromString("6.0.0")));
            builder.put(IndexMetadata.SETTING_VERSION_COMPATIBILITY, getRandomCompatibleVersion());
        }
        return builder;
    }

    private static Version getRandomCompatibleVersion() {
        return VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumIndexCompatibilityVersion(), Version.CURRENT);
    }

    public void testUpdatesNodeWithNewRoles() throws Exception {
        // Node roles vary by version, and new roles are suppressed for BWC. This means we can receive a join from a node that's already
        // in the cluster but with a different set of roles: the node didn't change roles, but the cluster state came via an older master.
        // In this case we must properly process its join to ensure that the roles are correct.

        final AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.adaptAutoExpandReplicas(any())).then(invocationOnMock -> invocationOnMock.getArguments()[0]);
        final RerouteService rerouteService = (reason, priority, listener) -> listener.onResponse(null);

        final JoinTaskExecutor joinTaskExecutor = new JoinTaskExecutor(allocationService, rerouteService);

        final DiscoveryNode masterNode = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);

        final DiscoveryNode actualNode = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNode bwcNode = new DiscoveryNode(
            actualNode.getName(),
            actualNode.getId(),
            actualNode.getEphemeralId(),
            actualNode.getHostName(),
            actualNode.getHostAddress(),
            actualNode.getAddress(),
            actualNode.getAttributes(),
            new HashSet<>(randomSubsetOf(actualNode.getRoles())),
            actualNode.getVersion()
        );
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(masterNode).localNodeId(masterNode.getId()).masterNodeId(masterNode.getId()).add(bwcNode))
            .build();

        final var resultingState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterState,
            joinTaskExecutor,
            List.of(
                JoinTask.singleNode(
                    actualNode,
                    "test",
                    ActionListener.wrap(() -> { throw new AssertionError("should not complete publication"); })
                )
            )
        );

        assertThat(resultingState.getNodes().get(actualNode.getId()).getRoles(), equalTo(actualNode.getRoles()));
    }
}
