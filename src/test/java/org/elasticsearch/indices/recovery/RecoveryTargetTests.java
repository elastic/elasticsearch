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
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class RecoveryTargetTests extends ElasticsearchSingleNodeTest {

    public void testFullRecoveryFromPre14() throws IOException {
        createIndex("test");
        int numDocs = scaledRandomIntBetween(10, 100);
        for (int j = 0; j < numDocs; ++j) {
            String id = Integer.toString(j);
            client().prepareIndex("test", "type1", id).setSource("text", "sometext").get();
        }
        client().admin().indices().prepareFlush("test").setWaitIfOngoing(true).setForce(true).get();
        RecoveryTarget recoveryTarget = getInstanceFromNode(RecoveryTarget.class);
        IndexService idxService = getInstanceFromNode(IndicesService.class).indexService("test");
        Store store = idxService.shard(0).store();
        store.incRef();
        try {
            DiscoveryNode discoveryNode = new DiscoveryNode("123", new LocalTransportAddress("123"), Version.CURRENT);
            Map<String, StoreFileMetaData> metaDataMap = recoveryTarget.existingFiles(discoveryNode, store).asMap();
            assertTrue(metaDataMap.size() > 0);
            int iters = randomIntBetween(10, 20);
            for (int i = 0; i < iters; i++) {
                Version version = randomVersion();
                DiscoveryNode discoNode = new DiscoveryNode("123", new LocalTransportAddress("123"), version);
                Map<String, StoreFileMetaData> map = recoveryTarget.existingFiles(discoNode, store).asMap();
                if (version.before(Version.V_1_4_0)) {
                    assertTrue(map.isEmpty());
                } else {
                    assertEquals(map.size(), metaDataMap.size());
                }

            }
        } finally {
            store.decRef();
        }

    }
}
