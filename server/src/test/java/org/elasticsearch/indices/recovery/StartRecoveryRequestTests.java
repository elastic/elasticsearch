/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collections;

import static java.util.Collections.emptyMap;
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
            DiscoveryNodeUtils.create("a", buildNewFakeTransportAddress(), emptyMap(), emptySet(), targetNodeVersion),
            DiscoveryNodeUtils.create("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), targetNodeVersion),
            metadataSnapshot,
            randomBoolean(),
            randomNonNegativeLong(),
            randomBoolean() || metadataSnapshot.getHistoryUUID() == null ? SequenceNumbers.UNASSIGNED_SEQ_NO : randomNonNegativeLong(),
            randomBoolean()
        );

        final ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        final OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        out.setTransportVersion(serializationVersion);
        outRequest.writeTo(out);

        final ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
        in.setTransportVersion(serializationVersion);
        final StartRecoveryRequest inRequest = new StartRecoveryRequest(in);

        assertThat(outRequest.shardId(), equalTo(inRequest.shardId()));
        assertThat(outRequest.targetAllocationId(), equalTo(inRequest.targetAllocationId()));
        assertThat(outRequest.sourceNode(), equalTo(inRequest.sourceNode()));
        assertThat(outRequest.targetNode(), equalTo(inRequest.targetNode()));
        assertThat(outRequest.metadataSnapshot().fileMetadataMap(), equalTo(inRequest.metadataSnapshot().fileMetadataMap()));
        assertThat(outRequest.isPrimaryRelocation(), equalTo(inRequest.isPrimaryRelocation()));
        assertThat(outRequest.recoveryId(), equalTo(inRequest.recoveryId()));
        assertThat(outRequest.startingSeqNo(), equalTo(inRequest.startingSeqNo()));
    }

    public void testDescription() {
        final var node = DiscoveryNodeUtils.builder("a").roles(emptySet()).build();
        assertEquals(
            "recovery of [index][0] to "
                + node.descriptionWithoutAttributes()
                + " [recoveryId=1, targetAllocationId=allocationId, startingSeqNo=-2, "
                + "primaryRelocation=false, canDownloadSnapshotFiles=true]",
            new StartRecoveryRequest(
                new ShardId("index", "uuid", 0),
                "allocationId",
                null,
                node,
                Store.MetadataSnapshot.EMPTY,
                false,
                1,
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                true
            ).getDescription()
        );
    }

}
