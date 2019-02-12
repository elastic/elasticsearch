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

package org.elasticsearch.indices.recovery;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.test.ESTestCase;

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
        Store.MetadataSnapshot metadataSnapshot = randomBoolean() ? Store.MetadataSnapshot.EMPTY :
            new Store.MetadataSnapshot(Collections.emptyMap(),
                Collections.singletonMap(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID()), randomIntBetween(0, 100));
        final StartRecoveryRequest outRequest = new StartRecoveryRequest(
                new ShardId("test", "_na_", 0),
                UUIDs.randomBase64UUID(),
                new DiscoveryNode("a", buildNewFakeTransportAddress(), emptyMap(), emptySet(), targetNodeVersion),
                new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), targetNodeVersion),
                metadataSnapshot,
                randomBoolean(),
                randomNonNegativeLong(),
                randomBoolean() || metadataSnapshot.getHistoryUUID() == null ?
                    SequenceNumbers.UNASSIGNED_SEQ_NO : randomNonNegativeLong());

        final ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        final OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        out.setVersion(targetNodeVersion);
        outRequest.writeTo(out);

        final ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
        in.setVersion(targetNodeVersion);
        final StartRecoveryRequest inRequest = new StartRecoveryRequest();
        inRequest.readFrom(in);

        assertThat(outRequest.shardId(), equalTo(inRequest.shardId()));
        assertThat(outRequest.targetAllocationId(), equalTo(inRequest.targetAllocationId()));
        assertThat(outRequest.sourceNode(), equalTo(inRequest.sourceNode()));
        assertThat(outRequest.targetNode(), equalTo(inRequest.targetNode()));
        assertThat(outRequest.metadataSnapshot().asMap(), equalTo(inRequest.metadataSnapshot().asMap()));
        assertThat(outRequest.isPrimaryRelocation(), equalTo(inRequest.isPrimaryRelocation()));
        assertThat(outRequest.recoveryId(), equalTo(inRequest.recoveryId()));
        if (targetNodeVersion.onOrAfter(Version.V_6_0_0_alpha1)) {
            assertThat(outRequest.startingSeqNo(), equalTo(inRequest.startingSeqNo()));
        } else {
            assertThat(SequenceNumbers.UNASSIGNED_SEQ_NO, equalTo(inRequest.startingSeqNo()));
        }
    }

}
