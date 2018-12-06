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

package org.elasticsearch.index.reindex;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class LegacyDeleteByQueryBasicTests extends ReindexTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    /**
     * Test delete by query support for filtering by type. This entire feature
     * can and should be removed when we drop support for types index with
     * multiple types from core.
     */
    public void testFilterByType() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id))); // allows for multiple types
        indexRandom(true,
                client().prepareIndex("test", "test1", "1").setSource("foo", "a"),
                client().prepareIndex("test", "test2", "2").setSource("foo", "a"),
                client().prepareIndex("test", "test2", "3").setSource("foo", "b"));

        assertHitCount(client().prepareSearch("test").setSize(0).get(), 3);

        // Deletes doc of the type "type2" that also matches foo:a
        DeleteByQueryRequestBuilder builder = deleteByQuery().source("test").filter(termQuery("foo", "a")).refresh(true);
        builder.source().setTypes("test2");
        assertThat(builder.get(), matcher().deleted(1));
        assertHitCount(client().prepareSearch("test").setSize(0).get(), 2);
    }

}
