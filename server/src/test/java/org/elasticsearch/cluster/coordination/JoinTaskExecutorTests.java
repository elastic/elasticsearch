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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import static org.elasticsearch.test.VersionUtils.maxCompatibleVersion;
import static org.elasticsearch.test.VersionUtils.randomCompatibleVersion;
import static org.elasticsearch.test.VersionUtils.randomVersionBetween;

public class JoinTaskExecutorTests extends ESTestCase {

    public void testPreventJoinClusterWithNewerIndices() {
        Settings.builder().build();
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1).build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
        JoinTaskExecutor.ensureIndexCompatibility(Version.CURRENT, metadata);

        expectThrows(IllegalStateException.class, () ->
        JoinTaskExecutor.ensureIndexCompatibility(VersionUtils.getPreviousVersion(Version.CURRENT),
            metadata));
    }

    public void testPreventJoinClusterWithUnsupportedIndices() {
        Settings.builder().build();
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.fromString("6.8.0"))) // latest V6 released version
            .numberOfShards(1)
            .numberOfReplicas(1).build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
        expectThrows(IllegalStateException.class, () ->
            JoinTaskExecutor.ensureIndexCompatibility(Version.CURRENT,
                metadata));
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

        Version oldMajor = minNodeVersion.minimumCompatibilityVersion();
        expectThrows(IllegalStateException.class, () -> JoinTaskExecutor.ensureMajorVersionBarrier(oldMajor, minNodeVersion));

        final Version minGoodVersion = maxNodeVersion.major == minNodeVersion.major ?
            // we have to stick with the same major
            minNodeVersion :
            maxNodeVersion.minimumCompatibilityVersion();
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
            .settings(settings(VersionUtils.randomVersionBetween(random(),
                Version.CURRENT.minimumIndexCompatibilityVersion(), Version.CURRENT)))
            .numberOfShards(1)
            .numberOfReplicas(1).build();
        metaBuilder.put(indexMetadata, false);
        indexMetadata = IndexMetadata.builder("test1")
            .settings(settings(VersionUtils.randomVersionBetween(random(),
                Version.CURRENT.minimumIndexCompatibilityVersion(), Version.CURRENT)))
            .numberOfShards(1)
            .numberOfReplicas(1).build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
            JoinTaskExecutor.ensureIndexCompatibility(Version.CURRENT,
                metadata);
    }
}
