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

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;

public class IndicesStatsTests extends ESSingleNodeTestCase {

    public void testSegmentStatsEmptyIndex() {
        createIndex("test");
        IndicesStatsResponse rsp = client().admin().indices().prepareStats("test").get();
        SegmentsStats stats = rsp.getTotal().getSegments();
        assertEquals(0, stats.getTermsMemoryInBytes());
        assertEquals(0, stats.getStoredFieldsMemoryInBytes());
        assertEquals(0, stats.getTermVectorsMemoryInBytes());
        assertEquals(0, stats.getNormsMemoryInBytes());
        assertEquals(0, stats.getPointsMemoryInBytes());
        assertEquals(0, stats.getDocValuesMemoryInBytes());
    }

    public void testSegmentStats() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("doc")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "keyword")
                            .field("doc_values", true)
                            .field("store", true)
                        .endObject()
                        .startObject("bar")
                            .field("type", "text")
                            .field("term_vector", "with_positions_offsets_payloads")
                        .endObject()
                        .startObject("baz")
                            .field("type", "long")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("doc", mapping));
        ensureGreen("test");
        client().prepareIndex("test", "doc", "1").setSource("foo", "bar", "bar", "baz", "baz", 42).get();
        client().admin().indices().prepareRefresh("test").get();

        IndicesStatsResponse rsp = client().admin().indices().prepareStats("test").get();
        SegmentsStats stats = rsp.getIndex("test").getTotal().getSegments();
        assertThat(stats.getTermsMemoryInBytes(), greaterThan(0L));
        assertThat(stats.getStoredFieldsMemoryInBytes(), greaterThan(0L));
        assertThat(stats.getTermVectorsMemoryInBytes(), greaterThan(0L));
        assertThat(stats.getNormsMemoryInBytes(), greaterThan(0L));
        assertThat(stats.getPointsMemoryInBytes(), greaterThan(0L));
        assertThat(stats.getDocValuesMemoryInBytes(), greaterThan(0L));

        // now check multiple segments stats are merged together
        client().prepareIndex("test", "doc", "2").setSource("foo", "bar", "bar", "baz", "baz", 43).get();
        client().admin().indices().prepareRefresh("test").get();

        rsp = client().admin().indices().prepareStats("test").get();
        SegmentsStats stats2 = rsp.getIndex("test").getTotal().getSegments();
        assertThat(stats2.getTermsMemoryInBytes(), greaterThan(stats.getTermsMemoryInBytes()));
        assertThat(stats2.getStoredFieldsMemoryInBytes(), greaterThan(stats.getStoredFieldsMemoryInBytes()));
        assertThat(stats2.getTermVectorsMemoryInBytes(), greaterThan(stats.getTermVectorsMemoryInBytes()));
        assertThat(stats2.getNormsMemoryInBytes(), greaterThan(stats.getNormsMemoryInBytes()));
        assertThat(stats2.getPointsMemoryInBytes(), greaterThan(stats.getPointsMemoryInBytes()));
        assertThat(stats2.getDocValuesMemoryInBytes(), greaterThan(stats.getDocValuesMemoryInBytes()));
    }

    public void testCommitStats() throws Exception {
        createIndex("test");
        ensureGreen("test");

        IndicesStatsResponse rsp = client().admin().indices().prepareStats("test").get();
        for (ShardStats shardStats : rsp.getIndex("test").getShards()) {
            final CommitStats commitStats = shardStats.getCommitStats();
            assertNotNull(commitStats);
            assertThat(commitStats.getGeneration(), greaterThan(0L));
            assertThat(commitStats.getId(), notNullValue());
            assertThat(commitStats.getUserData(), hasKey(Translog.TRANSLOG_GENERATION_KEY));
            assertThat(commitStats.getUserData(), hasKey(Translog.TRANSLOG_UUID_KEY));
        }
    }

}
