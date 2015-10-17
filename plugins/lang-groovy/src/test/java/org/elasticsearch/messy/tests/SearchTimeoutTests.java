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

package org.elasticsearch.messy.tests;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.groovy.GroovyPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@ESIntegTestCase.ClusterScope(scope= ESIntegTestCase.Scope.SUITE)
public class SearchTimeoutTests extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(GroovyPlugin.class);
    }
    
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.settingsBuilder().put(super.nodeSettings(nodeOrdinal)).build();
    }

    @Test
    public void simpleTimeoutTest() throws Exception {
        client().prepareIndex("test", "type", "1").setSource("field", "value").setRefresh(true).execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setTimeout(new TimeValue(10, TimeUnit.MILLISECONDS))
                .setQuery(scriptQuery(new Script("Thread.sleep(500); return true;")))
                .execute().actionGet();
        assertThat(searchResponse.isTimedOut(), equalTo(true));
    }
}
