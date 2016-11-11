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

package org.elasticsearch.update;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class TimestampTTLBWIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
                UpdateIT.FieldIncrementScriptPlugin.class,
                UpdateIT.ExtractContextInSourceScriptPlugin.class,
                UpdateIT.PutFieldValuesScriptPlugin.class,
                InternalSettingsPlugin.class
        );
    }

    public void testSort() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", true).endObject()
                .endObject().endObject();
        assertAcked(prepareCreate("test")
                .setSettings(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_3_0.id)
                .addMapping("type", mapping));
        ensureGreen();
        final int numDocs = randomIntBetween(10, 20);
        IndexRequestBuilder[] indexReqs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; ++i) {
            indexReqs[i] = client().prepareIndex("test", "type", Integer.toString(i)).setTimestamp(Integer.toString(randomInt(1000)))
                    .setSource();
        }
        indexRandom(true, indexReqs);

        SortOrder order = randomFrom(SortOrder.values());

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(randomIntBetween(1, numDocs + 5))
                .addSort("_timestamp", order)
                .addStoredField("_timestamp")
                .execute().actionGet();
        assertNoFailures(searchResponse);
        SearchHit[] hits = searchResponse.getHits().hits();
        Long previousTs = order == SortOrder.ASC ? 0 : Long.MAX_VALUE;
        for (int i = 0; i < hits.length; ++i) {
            SearchHitField timestampField = hits[i].getFields().get("_timestamp");
            Long timestamp = timestampField.<Long>getValue();
            assertThat(previousTs, order == SortOrder.ASC ? lessThanOrEqualTo(timestamp) : greaterThanOrEqualTo(timestamp));
            previousTs = timestamp;
        }
    }

    public void testUpdate() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias"))
                .setSettings(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_3_0.id)
                .addMapping("type1", XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("type1")
                        .startObject("_timestamp").field("enabled", true).endObject()
                        .startObject("_ttl").field("enabled", true).endObject()
                        .endObject()
                        .endObject()));

        ensureGreen();

        try {
            client().prepareUpdate(indexOrAlias(), "type1", "1")
                    .setScript(new Script(ScriptType.INLINE, "field_inc", "field", Collections.emptyMap())).execute().actionGet();
            fail();
        } catch (DocumentMissingException e) {
            // all is well
        }

        // check TTL is kept after an update without TTL
        client().prepareIndex("test", "type1", "2").setSource("field", 1).setTTL(86400000L).setRefreshPolicy(IMMEDIATE).get();
        GetResponse getResponse = client().prepareGet("test", "type1", "2").setStoredFields("_ttl").execute().actionGet();
        long ttl = ((Number) getResponse.getField("_ttl").getValue()).longValue();
        assertThat(ttl, greaterThan(0L));
        client().prepareUpdate(indexOrAlias(), "type1", "2")
                .setScript(new Script(ScriptType.INLINE, "field_inc", "field", Collections.emptyMap())).execute().actionGet();
        getResponse = client().prepareGet("test", "type1", "2").setStoredFields("_ttl").execute().actionGet();
        ttl = ((Number) getResponse.getField("_ttl").getValue()).longValue();
        assertThat(ttl, greaterThan(0L));

        // check TTL update
        client().prepareUpdate(indexOrAlias(), "type1", "2")
                .setScript(new Script(ScriptType.INLINE, "put_values", "",
                    Collections.singletonMap("_ctx", Collections.singletonMap("_ttl", 3600000)))).execute().actionGet();
        getResponse = client().prepareGet("test", "type1", "2").setStoredFields("_ttl").execute().actionGet();
        ttl = ((Number) getResponse.getField("_ttl").getValue()).longValue();
        assertThat(ttl, greaterThan(0L));
        assertThat(ttl, lessThanOrEqualTo(3600000L));

        // check timestamp update
        client().prepareIndex("test", "type1", "3").setSource("field", 1).setRefreshPolicy(IMMEDIATE).get();
        client().prepareUpdate(indexOrAlias(), "type1", "3")
                .setScript(new Script(ScriptType.INLINE, "put_values", "",
                    Collections.singletonMap("_ctx", Collections.singletonMap("_timestamp", "2009-11-15T14:12:12")))).execute()
                .actionGet();
        getResponse = client().prepareGet("test", "type1", "3").setStoredFields("_timestamp").execute().actionGet();
        long timestamp = ((Number) getResponse.getField("_timestamp").getValue()).longValue();
        assertThat(timestamp, equalTo(1258294332000L));
    }

    public void testContextVariables() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias"))
                        .setSettings(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_3_0.id)
                        .addMapping("type1", XContentFactory.jsonBuilder()
                                .startObject()
                                .startObject("type1")
                                .startObject("_timestamp").field("enabled", true).endObject()
                                .startObject("_ttl").field("enabled", true).endObject()
                                .endObject()
                                .endObject())
                        .addMapping("subtype1", XContentFactory.jsonBuilder()
                                .startObject()
                                .startObject("subtype1")
                                .startObject("_parent").field("type", "type1").endObject()
                                .startObject("_timestamp").field("enabled", true).endObject()
                                .startObject("_ttl").field("enabled", true).endObject()
                                .endObject()
                                .endObject())
        );
        ensureGreen();

        // Index some documents
        long timestamp = System.currentTimeMillis();
        client().prepareIndex()
                .setIndex("test")
                .setType("type1")
                .setId("parentId1")
                .setTimestamp(String.valueOf(timestamp-1))
                .setSource("field1", 0, "content", "bar")
                .execute().actionGet();

        long ttl = 10000;
        client().prepareIndex()
                .setIndex("test")
                .setType("subtype1")
                .setId("id1")
                .setParent("parentId1")
                .setRouting("routing1")
                .setTimestamp(String.valueOf(timestamp))
                .setTTL(ttl)
                .setSource("field1", 1, "content", "foo")
                .execute().actionGet();

        // Update the first object and note context variables values
        UpdateResponse updateResponse = client().prepareUpdate("test", "subtype1", "id1")
                .setRouting("routing1")
                .setScript(new Script(ScriptType.INLINE, "extract_ctx", "", Collections.emptyMap()))
                .execute().actionGet();

        assertEquals(2, updateResponse.getVersion());

        GetResponse getResponse = client().prepareGet("test", "subtype1", "id1").setRouting("routing1").execute().actionGet();
        Map<String, Object> updateContext = (Map<String, Object>) getResponse.getSourceAsMap().get("update_context");
        assertEquals("test", updateContext.get("_index"));
        assertEquals("subtype1", updateContext.get("_type"));
        assertEquals("id1", updateContext.get("_id"));
        assertEquals(1, updateContext.get("_version"));
        assertEquals("parentId1", updateContext.get("_parent"));
        assertEquals("routing1", updateContext.get("_routing"));
        assertThat(((Integer) updateContext.get("_ttl")).longValue(), allOf(greaterThanOrEqualTo(ttl-3000), lessThanOrEqualTo(ttl)));

        // Idem with the second object
        updateResponse = client().prepareUpdate("test", "type1", "parentId1")
                .setScript(new Script(ScriptType.INLINE, "extract_ctx", "", Collections.emptyMap()))
                .execute().actionGet();

        assertEquals(2, updateResponse.getVersion());

        getResponse = client().prepareGet("test", "type1", "parentId1").execute().actionGet();
        updateContext = (Map<String, Object>) getResponse.getSourceAsMap().get("update_context");
        assertEquals("test", updateContext.get("_index"));
        assertEquals("type1", updateContext.get("_type"));
        assertEquals("parentId1", updateContext.get("_id"));
        assertEquals(1, updateContext.get("_version"));
        assertNull(updateContext.get("_parent"));
        assertNull(updateContext.get("_routing"));
        assertNull(updateContext.get("_ttl"));
    }

    private static String indexOrAlias() {
        return randomBoolean() ? "test" : "alias";
    }
}
