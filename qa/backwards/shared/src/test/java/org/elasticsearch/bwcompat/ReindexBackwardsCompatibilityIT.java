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

package org.elasticsearch.bwcompat;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.reindex.BulkIndexByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.index.reindex.ReindexResponse;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESBackcompatTestCase;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.empty;

/**
 * Runs reindex across a mixed version cluster.
 */
@ESIntegTestCase.ClusterScope(minNumDataNodes = 1, maxNumDataNodes = 2, scope = ESIntegTestCase.Scope.SUITE,
        numClientNodes = 0, transportClientRatio = 0.0)
public class ReindexBackwardsCompatibilityIT extends ESBackcompatTestCase {
    @Override
    @SuppressWarnings("unchecked")
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(ReindexPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    public void testReindex() throws InterruptedException, ExecutionException {
        assumeTrue("Reindex isn't allowed on clusters with nodes before 2.3", backwardsCompatibilityVersion().onOrAfter(Version.V_2_3_0));

        indexRandom(true,
                client().prepareIndex("test", "test", "1").setSource("foo", "bar"),
                client().prepareIndex("test", "test", "2").setSource("foo", "bar"),
                client().prepareIndex("test", "test", "3").setSource("foo", "bar"));

        ReindexResponse response = ReindexAction.INSTANCE.newRequestBuilder(client()).source("test").destination("dest").refresh(true).get();
        assertThat(response.getIndexingFailures(), empty());
        assertThat(response.getSearchFailures(), empty());
        assertEquals(3, response.getCreated());

        SearchResponse searchResponse = client().prepareSearch("dest").setSize(0).get();
        assertEquals(3, searchResponse.getHits().totalHits());
    }

    public void testUpdateByQuery() throws InterruptedException, ExecutionException {
        assumeTrue("Reindex isn't allowed on clusters with nodes before 2.3", backwardsCompatibilityVersion().onOrAfter(Version.V_2_3_0));

        indexRandom(true,
                client().prepareIndex("test", "test", "1").setSource("foo", "bar"),
                client().prepareIndex("test", "test", "2").setSource("foo", "bar"),
                client().prepareIndex("test", "test", "3").setSource("foo", "bar"));

        BulkIndexByScrollResponse response = UpdateByQueryAction.INSTANCE.newRequestBuilder(client()).source("test").refresh(true)
                .get();
        assertThat(response.getIndexingFailures(), empty());
        assertThat(response.getSearchFailures(), empty());
        assertEquals(3, response.getUpdated());
    }
}
