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

package org.elasticsearch.index.shard;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

/** Unit test(s) for IndexShardModule */
public class IndexShardModuleTests extends ESTestCase {

    @Test
    public void testDetermineShadowEngineShouldBeUsed() {
        ShardId shardId = new ShardId("myindex", 0);
        Settings regularSettings = Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 2)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();

        Settings shadowSettings = Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 2)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .build();

        IndexShardModule ism1 = new IndexShardModule(shardId, true, regularSettings);
        IndexShardModule ism2 = new IndexShardModule(shardId, false, regularSettings);
        IndexShardModule ism3 = new IndexShardModule(shardId, true, shadowSettings);
        IndexShardModule ism4 = new IndexShardModule(shardId, false, shadowSettings);

        assertFalse("no shadow replicas for normal settings", ism1.useShadowEngine());
        assertFalse("no shadow replicas for normal settings", ism2.useShadowEngine());
        assertFalse("no shadow replicas for primary shard with shadow settings", ism3.useShadowEngine());
        assertTrue("shadow replicas for replica shards with shadow settings", ism4.useShadowEngine());
    }
}
