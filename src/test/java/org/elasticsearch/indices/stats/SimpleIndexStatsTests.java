/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.indices.stats;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Random;

import static org.hamcrest.Matchers.*;

/**
 *
 */
@ClusterScope(scope = Scope.SUITE, numNodes = 2)
public class SimpleIndexStatsTests extends ElasticsearchIntegrationTest {

    @Test
    public void simpleStats() throws Exception {
        // rely on 1 replica for this tests
        createIndex("test1");
        createIndex("test2");

        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        client().prepareIndex("test1", "type1", Integer.toString(1)).setSource("field", "value").execute().actionGet();
        client().prepareIndex("test1", "type2", Integer.toString(1)).setSource("field", "value").execute().actionGet();
        client().prepareIndex("test2", "type", Integer.toString(1)).setSource("field", "value").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        IndicesStatsResponse stats = client().admin().indices().prepareStats().execute().actionGet();
        assertThat(stats.getPrimaries().getDocs().getCount(), equalTo(3l));
        assertThat(stats.getTotal().getDocs().getCount(), equalTo(6l));
        assertThat(stats.getPrimaries().getIndexing().getTotal().getIndexCount(), equalTo(3l));
        assertThat(stats.getTotal().getIndexing().getTotal().getIndexCount(), equalTo(6l));
        assertThat(stats.getTotal().getStore(), notNullValue());
        // verify nulls
        assertThat(stats.getTotal().getMerge(), nullValue());
        assertThat(stats.getTotal().getFlush(), nullValue());
        assertThat(stats.getTotal().getRefresh(), nullValue());

        assertThat(stats.getIndex("test1").getPrimaries().getDocs().getCount(), equalTo(2l));
        assertThat(stats.getIndex("test1").getTotal().getDocs().getCount(), equalTo(4l));
        assertThat(stats.getIndex("test1").getPrimaries().getStore(), notNullValue());
        assertThat(stats.getIndex("test1").getPrimaries().getMerge(), nullValue());
        assertThat(stats.getIndex("test1").getPrimaries().getFlush(), nullValue());
        assertThat(stats.getIndex("test1").getPrimaries().getRefresh(), nullValue());

        assertThat(stats.getIndex("test2").getPrimaries().getDocs().getCount(), equalTo(1l));
        assertThat(stats.getIndex("test2").getTotal().getDocs().getCount(), equalTo(2l));

        // make sure that number of requests in progress is 0
        assertThat(stats.getIndex("test1").getTotal().getIndexing().getTotal().getIndexCurrent(), equalTo(0l));
        assertThat(stats.getIndex("test1").getTotal().getIndexing().getTotal().getDeleteCurrent(), equalTo(0l));
        assertThat(stats.getIndex("test1").getTotal().getSearch().getTotal().getFetchCurrent(), equalTo(0l));
        assertThat(stats.getIndex("test1").getTotal().getSearch().getTotal().getQueryCurrent(), equalTo(0l));

        // check flags
        stats = client().admin().indices().prepareStats()
                .setDocs(false)
                .setStore(false)
                .setIndexing(false)
                .setFlush(true)
                .setRefresh(true)
                .setMerge(true)
                .execute().actionGet();

        assertThat(stats.getTotal().getDocs(), nullValue());
        assertThat(stats.getTotal().getStore(), nullValue());
        assertThat(stats.getTotal().getIndexing(), nullValue());
        assertThat(stats.getTotal().getMerge(), notNullValue());
        assertThat(stats.getTotal().getFlush(), notNullValue());
        assertThat(stats.getTotal().getRefresh(), notNullValue());

        // check types
        stats = client().admin().indices().prepareStats().setTypes("type1", "type").execute().actionGet();
        assertThat(stats.getPrimaries().getIndexing().getTypeStats().get("type1").getIndexCount(), equalTo(1l));
        assertThat(stats.getPrimaries().getIndexing().getTypeStats().get("type").getIndexCount(), equalTo(1l));
        assertThat(stats.getPrimaries().getIndexing().getTypeStats().get("type2"), nullValue());
        assertThat(stats.getPrimaries().getIndexing().getTypeStats().get("type1").getIndexCurrent(), equalTo(0l));
        assertThat(stats.getPrimaries().getIndexing().getTypeStats().get("type1").getDeleteCurrent(), equalTo(0l));

        assertThat(stats.getTotal().getGet().getCount(), equalTo(0l));
        // check get
        GetResponse getResponse = client().prepareGet("test1", "type1", "1").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));

        stats = client().admin().indices().prepareStats().execute().actionGet();
        assertThat(stats.getTotal().getGet().getCount(), equalTo(1l));
        assertThat(stats.getTotal().getGet().getExistsCount(), equalTo(1l));
        assertThat(stats.getTotal().getGet().getMissingCount(), equalTo(0l));

        // missing get
        getResponse = client().prepareGet("test1", "type1", "2").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(false));

        stats = client().admin().indices().prepareStats().execute().actionGet();
        assertThat(stats.getTotal().getGet().getCount(), equalTo(2l));
        assertThat(stats.getTotal().getGet().getExistsCount(), equalTo(1l));
        assertThat(stats.getTotal().getGet().getMissingCount(), equalTo(1l));

        // clear all
        stats = client().admin().indices().prepareStats()
                .setDocs(false)
                .setStore(false)
                .setIndexing(false)
                .setFlush(true)
                .setRefresh(true)
                .setMerge(true)
                .clear() // reset defaults
                .execute().actionGet();

        assertThat(stats.getTotal().getDocs(), nullValue());
        assertThat(stats.getTotal().getStore(), nullValue());
        assertThat(stats.getTotal().getIndexing(), nullValue());
        assertThat(stats.getTotal().getGet(), nullValue());
        assertThat(stats.getTotal().getSearch(), nullValue());
    }

    @Test
    public void testMergeStats() {
        // rely on 1 replica for this tests
        createIndex("test1");

        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        // clear all
        IndicesStatsResponse stats = client().admin().indices().prepareStats()
                .setDocs(false)
                .setStore(false)
                .setIndexing(false)
                .setFlush(true)
                .setRefresh(true)
                .setMerge(true)
                .clear() // reset defaults
                .execute().actionGet();

        assertThat(stats.getTotal().getDocs(), nullValue());
        assertThat(stats.getTotal().getStore(), nullValue());
        assertThat(stats.getTotal().getIndexing(), nullValue());
        assertThat(stats.getTotal().getGet(), nullValue());
        assertThat(stats.getTotal().getSearch(), nullValue());

        for (int i = 0; i < 20; i++) {
            client().prepareIndex("test1", "type1", Integer.toString(i)).setSource("field", "value").execute().actionGet();
            client().prepareIndex("test1", "type2", Integer.toString(i)).setSource("field", "value").execute().actionGet();
            client().admin().indices().prepareFlush().execute().actionGet();
        }
        client().admin().indices().prepareOptimize().setWaitForMerge(true).setMaxNumSegments(1).execute().actionGet();
        stats = client().admin().indices().prepareStats()
                .setMerge(true)
                .execute().actionGet();

        assertThat(stats.getTotal().getMerge(), notNullValue());
        assertThat(stats.getTotal().getMerge().getTotal(), greaterThan(0l));
    }

    @Test
    public void testSegmentsStasts() {
        prepareCreate("test1", 2).setSettings("index.number_of_shards", 5, "index.number_of_replicas", 1).get();

        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().get();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        for (int i = 0; i < 20; i++) {
            index("test1", "type1", Integer.toString(i), "field", "value");
            index("test1", "type2", Integer.toString(i), "field", "value");
            client().admin().indices().prepareFlush().get();
        }
        client().admin().indices().prepareOptimize().setWaitForMerge(true).setMaxNumSegments(1).execute().actionGet();
        IndicesStatsResponse stats = client().admin().indices().prepareStats().setSegments(true).get();

        assertThat(stats.getTotal().getSegments(), notNullValue());
        assertThat(stats.getTotal().getSegments().getCount(), equalTo(10l));
    }

    @Test
    public void testAllFlags() throws Exception {
        // rely on 1 replica for this tests
        createIndex("test1");
        createIndex("test2");

        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().get();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        client().prepareIndex("test1", "type1", Integer.toString(1)).setSource("field", "value").execute().actionGet();
        client().prepareIndex("test1", "type2", Integer.toString(1)).setSource("field", "value").execute().actionGet();
        client().prepareIndex("test2", "type", Integer.toString(1)).setSource("field", "value").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();
        IndicesStatsRequestBuilder builder = client().admin().indices().prepareStats();
        Flag[] values = CommonStatsFlags.Flag.values();
        for (Flag flag : values) {
            set(flag, builder, false);
        }

        IndicesStatsResponse stats = builder.execute().actionGet();
        for (Flag flag : values) {
            assertThat(isSet(flag, stats.getPrimaries()), equalTo(false));
            assertThat(isSet(flag, stats.getTotal()), equalTo(false));
        }

        for (Flag flag : values) {
            set(flag, builder, true);
        }
        stats = builder.execute().actionGet();
        for (Flag flag : values) {
            assertThat(isSet(flag, stats.getPrimaries()), equalTo(true));
            assertThat(isSet(flag, stats.getTotal()), equalTo(true));
        }
        Random random = getRandom();
        EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
        for (Flag flag : values) {
            if (random.nextBoolean()) {
                flags.add(flag);
            }
        }


        for (Flag flag : values) {
            set(flag, builder, false); // clear all
        }

        for (Flag flag : flags) { // set the flags
            set(flag, builder, true);
        }
        stats = builder.execute().actionGet();
        for (Flag flag : flags) { // check the flags
            assertThat(isSet(flag, stats.getPrimaries()), equalTo(true));
            assertThat(isSet(flag, stats.getTotal()), equalTo(true));
        }

        for (Flag flag : EnumSet.complementOf(flags)) { // check the complement
            assertThat(isSet(flag, stats.getPrimaries()), equalTo(false));
            assertThat(isSet(flag, stats.getTotal()), equalTo(false));
        }

    }

    @Test
    public void testEncodeDecodeCommonStats() throws IOException {
        CommonStatsFlags flags = new CommonStatsFlags();
        Flag[] values = CommonStatsFlags.Flag.values();
        assertThat(flags.anySet(), equalTo(true));

        for (Flag flag : values) {
            flags.set(flag, false);
        }
        assertThat(flags.anySet(), equalTo(false));
        for (Flag flag : values) {
            flags.set(flag, true);
        }
        assertThat(flags.anySet(), equalTo(true));
        Random random = getRandom();
        flags.set(values[random.nextInt(values.length)], false);
        assertThat(flags.anySet(), equalTo(true));

        {
            BytesStreamOutput out = new BytesStreamOutput();
            flags.writeTo(out);
            out.close();
            BytesReference bytes = out.bytes();
            CommonStatsFlags readStats = CommonStatsFlags.readCommonStatsFlags(new BytesStreamInput(bytes));
            for (Flag flag : values) {
                assertThat(flags.isSet(flag), equalTo(readStats.isSet(flag)));
            }
        }

        {
            for (Flag flag : values) {
                flags.set(flag, random.nextBoolean());
            }
            BytesStreamOutput out = new BytesStreamOutput();
            flags.writeTo(out);
            out.close();
            BytesReference bytes = out.bytes();
            CommonStatsFlags readStats = CommonStatsFlags.readCommonStatsFlags(new BytesStreamInput(bytes));
            for (Flag flag : values) {
                assertThat(flags.isSet(flag), equalTo(readStats.isSet(flag)));
            }
        }
    }

    @Test
    public void testFlagOrdinalOrder() {
        Flag[] flags = new Flag[]{Flag.Store, Flag.Indexing, Flag.Get, Flag.Search, Flag.Merge, Flag.Flush, Flag.Refresh,
                Flag.FilterCache, Flag.IdCache, Flag.FieldData, Flag.Docs, Flag.Warmer, Flag.Completion, Flag.Segments};

        assertThat(flags.length, equalTo(Flag.values().length));
        for (int i = 0; i < flags.length; i++) {
            assertThat("ordinal has changed - this breaks the wire protocol. Only append to new values", i, equalTo(flags[i].ordinal()));
        }
    }

    private static void set(Flag flag, IndicesStatsRequestBuilder builder, boolean set) {
        switch (flag) {
            case Docs:
                builder.setDocs(set);
                break;
            case FieldData:
                builder.setFieldData(set);
                break;
            case FilterCache:
                builder.setFilterCache(set);
                break;
            case Flush:
                builder.setFlush(set);
                break;
            case Get:
                builder.setGet(set);
                break;
            case IdCache:
                builder.setIdCache(set);
                break;
            case Indexing:
                builder.setIndexing(set);
                break;
            case Merge:
                builder.setMerge(set);
                break;
            case Refresh:
                builder.setRefresh(set);
                break;
            case Search:
                builder.setSearch(set);
                break;
            case Store:
                builder.setStore(set);
                break;
            case Warmer:
                builder.setWarmer(set);
                break;
            case Completion:
                builder.setCompletion(set);
                break;
            case Segments:
                builder.setSegments(set);
                break;
            default:
                assert false : "new flag? " + flag;
                break;
        }
    }

    private static boolean isSet(Flag flag, CommonStats response) {
        switch (flag) {
            case Docs:
                return response.getDocs() != null;
            case FieldData:
                return response.getFieldData() != null;
            case FilterCache:
                return response.getFilterCache() != null;
            case Flush:
                return response.getFlush() != null;
            case Get:
                return response.getGet() != null;
            case IdCache:
                return response.getIdCache() != null;
            case Indexing:
                return response.getIndexing() != null;
            case Merge:
                return response.getMerge() != null;
            case Refresh:
                return response.getRefresh() != null;
            case Search:
                return response.getSearch() != null;
            case Store:
                return response.getStore() != null;
            case Warmer:
                return response.getWarmer() != null;
            case Completion:
                return response.getCompletion() != null;
            case Segments:
                return response.getSegments() != null;
            default:
                assert false : "new flag? " + flag;
                return false;
        }
    }

}
