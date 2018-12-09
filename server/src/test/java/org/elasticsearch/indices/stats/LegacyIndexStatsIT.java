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

package org.elasticsearch.indices.stats;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class LegacyIndexStatsIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testFieldDataFieldsParam() {
        assertAcked(client()
                .admin()
                .indices()
                .prepareCreate("test1")
                .setSettings(Settings.builder().put(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey(), Version.V_5_6_0))
                .addMapping("_doc", "bar", "type=text,fielddata=true", "baz", "type=text,fielddata=true")
                .get());

        ensureGreen();

        client().prepareIndex("test1", "_doc", Integer.toString(1)).setSource("{\"bar\":\"bar\",\"baz\":\"baz\"}", XContentType.JSON).get();
        client().prepareIndex("test1", "_doc", Integer.toString(2)).setSource("{\"bar\":\"bar\",\"baz\":\"baz\"}", XContentType.JSON).get();
        refresh();

        client().prepareSearch("_all").addSort("bar", SortOrder.ASC).addSort("baz", SortOrder.ASC).execute().actionGet();

        final IndicesStatsRequestBuilder builder = client().admin().indices().prepareStats();

        {
            final IndicesStatsResponse stats = builder.execute().actionGet();
            assertThat(stats.getTotal().fieldData.getMemorySizeInBytes(), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields(), is(nullValue()));
        }

        {
            final IndicesStatsResponse stats = builder.setFieldDataFields("bar").execute().actionGet();
            assertThat(stats.getTotal().fieldData.getMemorySizeInBytes(), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("bar"), is(true));
            assertThat(stats.getTotal().fieldData.getFields().get("bar"), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("baz"), is(false));
        }

        {
            final IndicesStatsResponse stats = builder.setFieldDataFields("bar", "baz").execute().actionGet();
            assertThat(stats.getTotal().fieldData.getMemorySizeInBytes(), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("bar"), is(true));
            assertThat(stats.getTotal().fieldData.getFields().get("bar"), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("baz"), is(true));
            assertThat(stats.getTotal().fieldData.getFields().get("baz"), greaterThan(0L));
        }

        {
            final IndicesStatsResponse stats = builder.setFieldDataFields("*").execute().actionGet();
            assertThat(stats.getTotal().fieldData.getMemorySizeInBytes(), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("bar"), is(true));
            assertThat(stats.getTotal().fieldData.getFields().get("bar"), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("baz"), is(true));
            assertThat(stats.getTotal().fieldData.getFields().get("baz"), greaterThan(0L));
        }

        {
            final IndicesStatsResponse stats = builder.setFieldDataFields("*r").execute().actionGet();
            assertThat(stats.getTotal().fieldData.getMemorySizeInBytes(), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("bar"), is(true));
            assertThat(stats.getTotal().fieldData.getFields().get("bar"), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("baz"), is(false));
        }

    }

    public void testSimpleStats() throws Exception {
        // this test has some type stats tests that can be removed in 7.0
        assertAcked(prepareCreate("test1")
                .setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0))); // allows for multiple types
        createIndex("test2");
        ensureGreen();

        client().prepareIndex("test1", "type1", Integer.toString(1)).setSource("field", "value").execute().actionGet();
        client().prepareIndex("test1", "type2", Integer.toString(1)).setSource("field", "value").execute().actionGet();
        client().prepareIndex("test2", "type", Integer.toString(1)).setSource("field", "value").execute().actionGet();
        refresh();

        NumShards test1 = getNumShards("test1");
        long test1ExpectedWrites = 2 * test1.dataCopies;
        NumShards test2 = getNumShards("test2");
        long test2ExpectedWrites = test2.dataCopies;
        long totalExpectedWrites = test1ExpectedWrites + test2ExpectedWrites;

        IndicesStatsResponse stats = client().admin().indices().prepareStats().execute().actionGet();
        assertThat(stats.getPrimaries().getDocs().getCount(), equalTo(3L));
        assertThat(stats.getTotal().getDocs().getCount(), equalTo(totalExpectedWrites));
        assertThat(stats.getPrimaries().getIndexing().getTotal().getIndexCount(), equalTo(3L));
        assertThat(stats.getPrimaries().getIndexing().getTotal().getIndexFailedCount(), equalTo(0L));
        assertThat(stats.getPrimaries().getIndexing().getTotal().isThrottled(), equalTo(false));
        assertThat(stats.getPrimaries().getIndexing().getTotal().getThrottleTime().millis(), equalTo(0L));
        assertThat(stats.getTotal().getIndexing().getTotal().getIndexCount(), equalTo(totalExpectedWrites));
        assertThat(stats.getTotal().getStore(), notNullValue());
        assertThat(stats.getTotal().getMerge(), notNullValue());
        assertThat(stats.getTotal().getFlush(), notNullValue());
        assertThat(stats.getTotal().getRefresh(), notNullValue());

        assertThat(stats.getIndex("test1").getPrimaries().getDocs().getCount(), equalTo(2L));
        assertThat(stats.getIndex("test1").getTotal().getDocs().getCount(), equalTo(test1ExpectedWrites));
        assertThat(stats.getIndex("test1").getPrimaries().getStore(), notNullValue());
        assertThat(stats.getIndex("test1").getPrimaries().getMerge(), notNullValue());
        assertThat(stats.getIndex("test1").getPrimaries().getFlush(), notNullValue());
        assertThat(stats.getIndex("test1").getPrimaries().getRefresh(), notNullValue());

        assertThat(stats.getIndex("test2").getPrimaries().getDocs().getCount(), equalTo(1L));
        assertThat(stats.getIndex("test2").getTotal().getDocs().getCount(), equalTo(test2ExpectedWrites));

        // make sure that number of requests in progress is 0
        assertThat(stats.getIndex("test1").getTotal().getIndexing().getTotal().getIndexCurrent(), equalTo(0L));
        assertThat(stats.getIndex("test1").getTotal().getIndexing().getTotal().getDeleteCurrent(), equalTo(0L));
        assertThat(stats.getIndex("test1").getTotal().getSearch().getTotal().getFetchCurrent(), equalTo(0L));
        assertThat(stats.getIndex("test1").getTotal().getSearch().getTotal().getQueryCurrent(), equalTo(0L));

        // check flags
        stats = client().admin().indices().prepareStats().clear()
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
        assertThat(stats.getPrimaries().getIndexing().getTypeStats().get("type1").getIndexCount(), equalTo(1L));
        assertThat(stats.getPrimaries().getIndexing().getTypeStats().get("type").getIndexCount(), equalTo(1L));
        assertThat(stats.getPrimaries().getIndexing().getTypeStats().get("type1").getIndexFailedCount(), equalTo(0L));
        assertThat(stats.getPrimaries().getIndexing().getTypeStats().get("type2"), nullValue());
        assertThat(stats.getPrimaries().getIndexing().getTypeStats().get("type1").getIndexCurrent(), equalTo(0L));
        assertThat(stats.getPrimaries().getIndexing().getTypeStats().get("type1").getDeleteCurrent(), equalTo(0L));


        assertThat(stats.getTotal().getGet().getCount(), equalTo(0L));
        // check get
        GetResponse getResponse = client().prepareGet("test1", "type1", "1").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));

        stats = client().admin().indices().prepareStats().execute().actionGet();
        assertThat(stats.getTotal().getGet().getCount(), equalTo(1L));
        assertThat(stats.getTotal().getGet().getExistsCount(), equalTo(1L));
        assertThat(stats.getTotal().getGet().getMissingCount(), equalTo(0L));

        // missing get
        getResponse = client().prepareGet("test1", "type1", "2").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(false));

        stats = client().admin().indices().prepareStats().execute().actionGet();
        assertThat(stats.getTotal().getGet().getCount(), equalTo(2L));
        assertThat(stats.getTotal().getGet().getExistsCount(), equalTo(1L));
        assertThat(stats.getTotal().getGet().getMissingCount(), equalTo(1L));

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

        // index failed
        try {
            client().prepareIndex("test1", "type1", Integer.toString(1)).setSource("field", "value").setVersion(1)
                    .setVersionType(VersionType.EXTERNAL).execute().actionGet();
            fail("Expected a version conflict");
        } catch (VersionConflictEngineException e) {}
        try {
            client().prepareIndex("test1", "type2", Integer.toString(1)).setSource("field", "value").setVersion(1)
                    .setVersionType(VersionType.EXTERNAL).execute().actionGet();
            fail("Expected a version conflict");
        } catch (VersionConflictEngineException e) {}
        try {
            client().prepareIndex("test2", "type", Integer.toString(1)).setSource("field", "value").setVersion(1)
                    .setVersionType(VersionType.EXTERNAL).execute().actionGet();
            fail("Expected a version conflict");
        } catch (VersionConflictEngineException e) {}

        stats = client().admin().indices().prepareStats().setTypes("type1", "type2").execute().actionGet();
        assertThat(stats.getIndex("test1").getPrimaries().getIndexing().getTotal().getIndexFailedCount(), equalTo(2L));
        assertThat(stats.getIndex("test2").getPrimaries().getIndexing().getTotal().getIndexFailedCount(), equalTo(1L));
        assertThat(stats.getPrimaries().getIndexing().getTypeStats().get("type1").getIndexFailedCount(), equalTo(1L));
        assertThat(stats.getPrimaries().getIndexing().getTypeStats().get("type2").getIndexFailedCount(), equalTo(1L));
        assertThat(stats.getPrimaries().getIndexing().getTotal().getIndexFailedCount(), equalTo(3L));
    }


}
