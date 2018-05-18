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

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;

public class PeerRecoverySourceServiceTests extends IndexShardTestCase {

    public void testDuplicateRecoveries() throws IOException {
        IndexShard primary = newStartedShard(true);
        PeerRecoverySourceService peerRecoverySourceService = new PeerRecoverySourceService(Settings.EMPTY,
            mock(TransportService.class), mock(IndicesService.class),
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)));
        StartRecoveryRequest startRecoveryRequest = new StartRecoveryRequest(primary.shardId(), randomAlphaOfLength(10),
            getFakeDiscoNode("source"), getFakeDiscoNode("target"), Store.MetadataSnapshot.EMPTY, randomBoolean(), randomLong(),
            SequenceNumbers.UNASSIGNED_SEQ_NO);
        RecoverySourceHandler handler = peerRecoverySourceService.ongoingRecoveries.addNewRecovery(startRecoveryRequest, primary);
        DelayRecoveryException delayRecoveryException = expectThrows(DelayRecoveryException.class,
            () -> peerRecoverySourceService.ongoingRecoveries.addNewRecovery(startRecoveryRequest, primary));
        assertThat(delayRecoveryException.getMessage(), containsString("recovery with same target already registered"));
        peerRecoverySourceService.ongoingRecoveries.remove(primary, handler);
        // re-adding after removing previous attempt works
        handler = peerRecoverySourceService.ongoingRecoveries.addNewRecovery(startRecoveryRequest, primary);
        peerRecoverySourceService.ongoingRecoveries.remove(primary, handler);
        closeShards(primary);
    }
}
