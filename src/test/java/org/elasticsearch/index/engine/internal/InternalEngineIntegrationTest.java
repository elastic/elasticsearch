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

package org.elasticsearch.index.engine.internal;

import com.google.common.base.Predicate;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.util.BloomFilter;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.merge.policy.AbstractMergePolicyProvider;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class InternalEngineIntegrationTest extends ElasticsearchIntegrationTest {

    @Test
    @Slow
    public void testSettingLoadBloomFilterDefaultTrue() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.builder().put("number_of_replicas", 0).put("number_of_shards", 1)).get();
        client().prepareIndex("test", "foo").setSource("field", "foo").get();
        ensureGreen();
        refresh();
        IndicesStatsResponse stats = client().admin().indices().prepareStats().setSegments(true).get();
        final long segmentsMemoryWithBloom = stats.getTotal().getSegments().getMemoryInBytes();
        logger.info("segments with bloom: {}", segmentsMemoryWithBloom);

        logger.info("updating the setting to unload bloom filters");
        client().admin().indices().prepareUpdateSettings("test").setSettings(ImmutableSettings.builder().put(CodecService.INDEX_CODEC_BLOOM_LOAD, false)).get();
        logger.info("waiting for memory to match without blooms");
        awaitBusy(new Predicate<Object>() {
            public boolean apply(Object o) {
                IndicesStatsResponse stats = client().admin().indices().prepareStats().setSegments(true).get();
                long segmentsMemoryWithoutBloom = stats.getTotal().getSegments().getMemoryInBytes();
                logger.info("trying segments without bloom: {}", segmentsMemoryWithoutBloom);
                return segmentsMemoryWithoutBloom == (segmentsMemoryWithBloom - BloomFilter.Factory.DEFAULT.createFilter(1).getSizeInBytes());
            }
        });

        logger.info("updating the setting to load bloom filters");
        client().admin().indices().prepareUpdateSettings("test").setSettings(ImmutableSettings.builder().put(CodecService.INDEX_CODEC_BLOOM_LOAD, true)).get();
        logger.info("waiting for memory to match with blooms");
        awaitBusy(new Predicate<Object>() {
            public boolean apply(Object o) {
                IndicesStatsResponse stats = client().admin().indices().prepareStats().setSegments(true).get();
                long newSegmentsMemoryWithBloom = stats.getTotal().getSegments().getMemoryInBytes();
                logger.info("trying segments with bloom: {}", newSegmentsMemoryWithBloom);
                return newSegmentsMemoryWithBloom == segmentsMemoryWithBloom;
            }
        });
    }

    @Test
    @Slow
    public void testSettingLoadBloomFilterDefaultFalse() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.builder().put("number_of_replicas", 0).put("number_of_shards", 1).put(CodecService.INDEX_CODEC_BLOOM_LOAD, false)).get();
        client().prepareIndex("test", "foo").setSource("field", "foo").get();
        ensureGreen();
        refresh();

        IndicesStatsResponse stats = client().admin().indices().prepareStats().setSegments(true).get();
        final long segmentsMemoryWithoutBloom = stats.getTotal().getSegments().getMemoryInBytes();
        logger.info("segments without bloom: {}", segmentsMemoryWithoutBloom);

        logger.info("updating the setting to load bloom filters");
        client().admin().indices().prepareUpdateSettings("test").setSettings(ImmutableSettings.builder().put(CodecService.INDEX_CODEC_BLOOM_LOAD, true)).get();
        logger.info("waiting for memory to match with blooms");
        awaitBusy(new Predicate<Object>() {
            public boolean apply(Object o) {
                IndicesStatsResponse stats = client().admin().indices().prepareStats().setSegments(true).get();
                long segmentsMemoryWithBloom = stats.getTotal().getSegments().getMemoryInBytes();
                logger.info("trying segments with bloom: {}", segmentsMemoryWithoutBloom);
                return segmentsMemoryWithoutBloom == (segmentsMemoryWithBloom - BloomFilter.Factory.DEFAULT.createFilter(1).getSizeInBytes());
            }
        });

        logger.info("updating the setting to unload bloom filters");
        client().admin().indices().prepareUpdateSettings("test").setSettings(ImmutableSettings.builder().put(CodecService.INDEX_CODEC_BLOOM_LOAD, false)).get();
        logger.info("waiting for memory to match without blooms");
        awaitBusy(new Predicate<Object>() {
            public boolean apply(Object o) {
                IndicesStatsResponse stats = client().admin().indices().prepareStats().setSegments(true).get();
                long newSegmentsMemoryWithoutBloom = stats.getTotal().getSegments().getMemoryInBytes();
                logger.info("trying segments without bloom: {}", newSegmentsMemoryWithoutBloom);
                return newSegmentsMemoryWithoutBloom == segmentsMemoryWithoutBloom;
            }
        });
    }

    @Test
    public void testSetIndexCompoundOnFlush() {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.builder().put("number_of_replicas", 0).put("number_of_shards", 1)).get();
        client().prepareIndex("test", "foo").setSource("field", "foo").get();
        refresh();
        assertTotalCompoundSegments(1, 1, "test");
        client().admin().indices().prepareUpdateSettings("test")
                .setSettings(ImmutableSettings.builder().put(InternalEngine.INDEX_COMPOUND_ON_FLUSH, false)).get();
        client().prepareIndex("test", "foo").setSource("field", "foo").get();
        refresh();
        assertTotalCompoundSegments(1, 2, "test");

        client().admin().indices().prepareUpdateSettings("test")
                .setSettings(ImmutableSettings.builder().put(InternalEngine.INDEX_COMPOUND_ON_FLUSH, true)).get();
        client().prepareIndex("test", "foo").setSource("field", "foo").get();
        refresh();
        assertTotalCompoundSegments(2, 3, "test");
    }

    public void testForceOptimize() throws ExecutionException, InterruptedException {
        boolean compound = randomBoolean();
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.builder()
                .put("number_of_replicas", 0)
                .put("number_of_shards", 1)
                 // this is important otherwise the MP will still trigger a merge even if there is only one segment
                .put(InternalEngine.INDEX_COMPOUND_ON_FLUSH, compound)
                .put(AbstractMergePolicyProvider.INDEX_COMPOUND_FORMAT, compound)
        ));
        final int numDocs = randomIntBetween(10, 100);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test", "type").setSource("field", "value");
        }
        indexRandom(true, builders);
        ensureGreen();
        flushAndRefresh();
        client().admin().indices().prepareOptimize("test").setMaxNumSegments(1).setWaitForMerge(true).get();
        IndexSegments firstSegments = client().admin().indices().prepareSegments("test").get().getIndices().get("test");
        client().admin().indices().prepareOptimize("test").setMaxNumSegments(1).setWaitForMerge(true).get();
        IndexSegments secondsSegments = client().admin().indices().prepareSegments("test").get().getIndices().get("test");

        assertThat(segments(firstSegments), Matchers.containsInAnyOrder(segments(secondsSegments).toArray()));
        assertThat(segments(firstSegments).size(), Matchers.equalTo(1));
        assertThat(segments(secondsSegments), Matchers.containsInAnyOrder(segments(firstSegments).toArray()));
        assertThat(segments(secondsSegments).size(), Matchers.equalTo(1));
        client().admin().indices().prepareOptimize("test").setMaxNumSegments(1).setWaitForMerge(true).setForce(true).get();
        IndexSegments thirdSegments = client().admin().indices().prepareSegments("test").get().getIndices().get("test");
        assertThat(segments(firstSegments).size(), Matchers.equalTo(1));
        assertThat(segments(thirdSegments).size(), Matchers.equalTo(1));
        assertThat(segments(firstSegments), Matchers.not(Matchers.containsInAnyOrder(segments(thirdSegments).toArray())));
        assertThat(segments(thirdSegments), Matchers.not(Matchers.containsInAnyOrder(segments(firstSegments).toArray())));
    }

    private void assertTotalCompoundSegments(int i, int t, String index) {
        IndicesSegmentResponse indicesSegmentResponse = client().admin().indices().prepareSegments(index).get();
        IndexSegments indexSegments = indicesSegmentResponse.getIndices().get(index);
        Collection<IndexShardSegments> values = indexSegments.getShards().values();
        int compounds = 0;
        int total = 0;
        for (IndexShardSegments indexShardSegments : values) {
            for (ShardSegments s : indexShardSegments) {
                for (Segment segment : s) {
                    if (segment.isSearch() && segment.getNumDocs() > 0) {
                        if (segment.isCompound()) {
                            compounds++;
                        }
                        total++;
                    }
                }
            }
        }
        assertThat(compounds, Matchers.equalTo(i));
        assertThat(total, Matchers.equalTo(t));
    }

    private Set<Segment> segments(IndexSegments segments) {
        Set<Segment> segmentSet = new HashSet<>();
        for (IndexShardSegments s : segments) {
            for (ShardSegments shardSegments : s) {
                segmentSet.addAll(shardSegments.getSegments());
            }
        }
        return segmentSet;
    }
}
