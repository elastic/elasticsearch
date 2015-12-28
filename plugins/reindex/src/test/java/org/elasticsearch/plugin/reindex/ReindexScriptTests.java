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

package org.elasticsearch.plugin.reindex;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.index.VersionType.EXTERNAL;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.plugin.reindex.ReindexCornerCaseTests.fetchTimestamp;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

/**
 * Tests index-by-search with a script modifying the documents.
 */
public class ReindexScriptTests extends ReindexTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(SetBarScript.RegistrationPlugin.class);
        plugins.add(SetCtxFieldScript.RegistrationPlugin.class);
        return plugins;
    }

    public void testTransformed() throws Exception {
        createTestData();
        reindex("set-bar", "to", "cat");
        assertSearchHits(client().prepareSearch("dest").setQuery(matchQuery("bar", "cat")).get(), "test");
    }

    public void testAddingJunkToCtxIsError() throws Exception {
        createTestData();
        try {
            reindex("set-ctx-field", "junk", "cat");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Invalid fields added to ctx [junk]"));
        }
    }

    public void testSetIndex() throws Exception {
        createTestData();
        reindex("set-ctx-field", "_index", "dest2");
        assertSearchHits(client().prepareSearch("dest2").setQuery(matchQuery("foo", "a")).get(), "test");
    }

    public void testSettingIndexToNullIsError() throws Exception {
        createTestData();
        try {
            reindex("set-ctx-field", "_index", null);
        } catch (NullPointerException e) {
            assertThat(e.getMessage(), containsString("Can't reindex without a destination index!"));
        }
    }

    public void testSetType() throws Exception {
        createTestData();
        reindex("set-ctx-field", "_type", "test2");
        assertNoSearchHits(client().prepareSearch("dest").setTypes("test").get());
        assertSearchHits(client().prepareSearch("dest").setTypes("test2").setQuery(matchQuery("foo", "a")).get(), "test");
    }

    public void testSettingTypeToNullIsError() throws Exception {
        createTestData();
        try {
            reindex("set-ctx-field", "_type", null);
        } catch (NullPointerException e) {
            assertThat(e.getMessage(), containsString("Can't reindex without a destination type!"));
        }
    }

    public void testSetId() throws Exception {
        createTestData();
        reindex("set-ctx-field", "_id", "sparrow");
        assertEquals("sparrow", client().prepareSearch("dest").setQuery(matchQuery("foo", "a")).get().getHits().getAt(0).getId());
    }

    public void testSettingIdToNullGetsAnAutomaticallyGeneratedId() throws Exception {
        createTestData();
        reindex("set-ctx-field", "_id", null);
        assertThat(client().prepareSearch("dest").setQuery(matchQuery("foo", "a")).get().getHits().getAt(0).getId(), not(equalTo(1)));
    }

    public void testSetVersion() throws Exception {
        createTestData();
        reindex("set-ctx-field", "_version", randomFrom(new Object[] { 234, 234l }));
        assertEquals(234,
                client().prepareSearch("dest").setQuery(matchQuery("foo", "a")).setVersion(true).get().getHits().getAt(0).version());
    }

    public void testSetVersionToNullForcesOverwrite() throws Exception {
        createTestData();
        reindex("set-ctx-field", "_version", null);
        assertEquals(1,
                client().prepareSearch("dest").setQuery(matchQuery("foo", "a")).setVersion(true).get().getHits().getAt(0).version());
    }

    public void testSettingVersionToJunkIsAnError() throws Exception {
        createTestData();
        Object junkVersion = randomFrom(new Object[] { "junk", Math.PI });
        try {
            reindex("set-ctx-field", "_version", junkVersion);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("_version may only be set to an int or a long but was ["));
            assertThat(e.getMessage(), containsString(junkVersion.toString()));
        }
    }

    public void testSetParent() throws Exception {
        createTestData(false, true);
        reindex("set-ctx-field", "_parent", "cat");
        assertEquals("cat",
                client().prepareSearch("dest").setQuery(matchQuery("foo", "a")).get().getHits().getAt(0).field("_parent").value());
    }

    public void testSetRouting() throws Exception {
        createTestData();
        reindex("set-ctx-field", "_routing", "pancake");
        assertEquals("pancake",
                client().prepareSearch("dest").setQuery(matchQuery("foo", "a")).get().getHits().getAt(0).field("_routing").value());
    }

    public void testSetTimestamp() throws Exception {
        createTestData(true, false);
        assertEquals((Long) 10l, fetchTimestamp("src")); // Was 10
        reindex("set-ctx-field", "_timestamp", "1234");
        assertEquals((Long) 1234l, fetchTimestamp("dest")); // Changed! See?
    }

    public void testSettingTimestampToNullDefaultsToNow() throws Exception {
        createTestData(true, false);
        assertEquals((Long) 10l, fetchTimestamp("src")); // Was 10
        reindex("set-ctx-field", "_timestamp", null);
        assertThat(fetchTimestamp("dest"), greaterThan(10l)); // Now now-ish
    }

    public void testSetTtl() throws Exception {
        createTestData();
        reindex("set-ctx-field", "_ttl", randomFrom(new Object[] { 1233214, 134143797143L }));
        assertThat((Long) client().prepareSearch("dest").setQuery(matchQuery("foo", "a")).get().getHits().getAt(0).field("_ttl").value(),
                greaterThan(0l));
    }

    public void testSettingTtlToJunkIsAnError() throws Exception {
        createTestData();
        Object junkTtl = randomFrom(new Object[] { "junk", Math.PI });
        try {
            reindex("set-ctx-field", "_ttl", junkTtl);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("_ttl may only be set to an int or a long but was ["));
            assertThat(e.getMessage(), containsString(junkTtl.toString()));
        }
    }

    private void createTestData() throws Exception {
        createTestData(false, false);
    }

    private void createTestData(boolean timestamp, boolean parent) throws Exception {
        createIndex("src", timestamp, false);
        createIndex("dest", timestamp, parent);
        ensureYellow();
        IndexRequestBuilder doc = client().prepareIndex("src", "test", "test").setSource("foo", "a")
                // These are useful for testing modifying the version
                .setVersion(10).setVersionType(VersionType.EXTERNAL);
        if (timestamp) {
            doc.setTimestamp("10");
        }
        indexRandom(true, doc);
    }

    private void createIndex(String name, boolean timestamp, boolean parent) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("test");
        if (timestamp) {
            mapping.startObject("_timestamp").field("enabled", true).endObject();
        }
        if (parent) {
            mapping.startObject("_parent").field("type", "parent").endObject();
        }
        mapping.startObject("_ttl").field("enabled", true).endObject();
        mapping.endObject().endObject();
        assertAcked(client().admin().indices().prepareCreate(name).addMapping("test", mapping));

    }

    private void reindex(String script, String paramKey, Object paramValue) throws Exception {
        ReindexRequestBuilder request = reindex().source("src").destination("dest").refresh(true)
                .script(new Script(script, ScriptType.INLINE, "native", singletonMap(paramKey, paramValue)));
        /*
         * Copy versions for all script so we can test those that intentionally
         * change them.
         */
        request.destination().setVersionType(EXTERNAL);
        ReindexResponse response = request.get();
        assertThat(response, responseMatcher().created(1));
    }
}