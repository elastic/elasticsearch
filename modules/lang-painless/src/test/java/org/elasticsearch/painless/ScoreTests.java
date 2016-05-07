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

package org.elasticsearch.painless;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collection;

public class ScoreTests extends ESSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(PainlessPlugin.class);
    }

    public void testScore() {
        createIndex("test", Settings.EMPTY, "type", "t", "type=text");
        ensureGreen("test");

        client().prepareIndex("test", "type", "1").setSource("t", "a").get();
        client().prepareIndex("test", "type", "2").setSource("t", "a a b").get();
        client().prepareIndex("test", "type", "3").setSource("t", "a a a b c").get();
        client().prepareIndex("test", "type", "4").setSource("t", "a b c d").get();
        client().prepareIndex("test", "type", "5").setSource("t", "a a b c d e").get();
        client().admin().indices().prepareRefresh("test").get();

        final Script script = new Script("score + 1", ScriptService.ScriptType.INLINE, "painless", null);

        final SearchResponse sr = client().prepareSearch("test").setQuery(
            QueryBuilders.functionScoreQuery(QueryBuilders.matchQuery("t", "a"),
                ScoreFunctionBuilders.scriptFunction(script))).get();
        final SearchHit[] hits = sr.getHits().getHits();

        for (final SearchHit hit : hits) {
            assertTrue(hit.score() > 0.9999F && hit.score() < 2.0001F);
        }

        assertEquals("1", hits[0].getId());
        assertEquals("3", hits[1].getId());
        assertEquals("2", hits[2].getId());
        assertEquals("5", hits[3].getId());
        assertEquals("4", hits[4].getId());
    }
}
