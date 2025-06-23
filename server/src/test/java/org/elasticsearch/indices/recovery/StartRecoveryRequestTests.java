/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.util.Collections;

import static java.util.Collections.emptySet;
import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.Matchers.equalTo;

public class StartRecoveryRequestTests extends ESTestCase {

    public void testSerialization() throws Exception {
        final Version targetNodeVersion = randomVersion(random());
        TransportVersion serializationVersion = TransportVersionUtils.randomVersion(random());
        Store.MetadataSnapshot metadataSnapshot = randomBoolean()
            ? Store.MetadataSnapshot.EMPTY
            : new Store.MetadataSnapshot(
                Collections.emptyMap(),
                Collections.singletonMap(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID()),
                randomIntBetween(0, 100)
            );
        final StartRecoveryRequest outRequest = new StartRecoveryRequest(
            new ShardId("test", "_na_", 0),
            UUIDs.randomBase64UUID(),
            DiscoveryNodeUtils.builder("a")
                .roles(emptySet())
                .version(targetNodeVersion, IndexVersions.ZERO, IndexVersion.current())
                .build(),
            DiscoveryNodeUtils.builder("b")
                .roles(emptySet())
                .version(targetNodeVersion, IndexVersions.ZERO, IndexVersion.current())
                .build(),
            randomNonNegativeLong(),
            metadataSnapshot,
            randomBoolean(),
            randomNonNegativeLong(),
            randomBoolean() || metadataSnapshot.getHistoryUUID() == null ? SequenceNumbers.UNASSIGNED_SEQ_NO : randomNonNegativeLong(),
            randomBoolean()
        );

        final StartRecoveryRequest inRequest = copyWriteable(
            outRequest,
            writableRegistry(),
            StartRecoveryRequest::new,
            serializationVersion
        );

        assertThat(outRequest.shardId(), equalTo(inRequest.shardId()));
        assertThat(outRequest.targetAllocationId(), equalTo(inRequest.targetAllocationId()));
        assertThat(outRequest.sourceNode(), equalTo(inRequest.sourceNode()));
        assertThat(outRequest.targetNode(), equalTo(inRequest.targetNode()));
        assertThat(outRequest.metadataSnapshot().fileMetadataMap(), equalTo(inRequest.metadataSnapshot().fileMetadataMap()));
        assertThat(outRequest.isPrimaryRelocation(), equalTo(inRequest.isPrimaryRelocation()));
        assertThat(outRequest.recoveryId(), equalTo(inRequest.recoveryId()));
        assertThat(outRequest.startingSeqNo(), equalTo(inRequest.startingSeqNo()));

        if (serializationVersion.onOrAfter(TransportVersions.V_8_11_X)) {
            assertEquals(outRequest.clusterStateVersion(), inRequest.clusterStateVersion());
        } else {
            assertEquals(0L, inRequest.clusterStateVersion());
        }
    }

    public void testDescription() {
        final var node = DiscoveryNodeUtils.builder("a").roles(emptySet()).build();
        assertEquals(
            "recovery of [index][0] to "
                + node.descriptionWithoutAttributes()
                + " [recoveryId=1, targetAllocationId=allocationId, clusterStateVersion=3, startingSeqNo=-2, "
                + "primaryRelocation=false, canDownloadSnapshotFiles=true]",
            new StartRecoveryRequest(
                new ShardId("index", "uuid", 0),
                "allocationId",
                null,
                node,
                3,
                Store.MetadataSnapshot.EMPTY,
                false,
                1,
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                true
            ).getDescription()
        );
    }

}
