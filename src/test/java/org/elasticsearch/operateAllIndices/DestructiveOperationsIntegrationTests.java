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

package org.elasticsearch.operateAllIndices;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST)
public class DestructiveOperationsIntegrationTests extends ElasticsearchIntegrationTest {

    @Test
    // One test for test performance, since cluster scope is test
    // The cluster scope is test b/c we can't clear cluster settings.
    public void testDestructiveOperations() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put(DestructiveOperations.REQUIRES_NAME, true)
                .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));

        assertAcked(client().admin().indices().prepareCreate("index1").get());
        assertAcked(client().admin().indices().prepareCreate("1index").get());

        // Should succeed, since no wildcards
        assertAcked(client().admin().indices().prepareDelete("1index").get());

        try {
            // should fail since index1 is the only index.
            client().admin().indices().prepareDelete("i*").get();
            fail();
        } catch (ElasticsearchIllegalArgumentException e) {}

        try {
            client().admin().indices().prepareDelete("_all").get();
            fail();
        } catch (ElasticsearchIllegalArgumentException e) {}

        settings = ImmutableSettings.builder()
                .put(DestructiveOperations.REQUIRES_NAME, false)
                .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));

        assertAcked(client().admin().indices().prepareDelete("_all").get());
        assertThat(client().admin().indices().prepareExists("_all").get().isExists(), equalTo(false));

        // end delete index:
        // close index:
        settings = ImmutableSettings.builder()
                .put(DestructiveOperations.REQUIRES_NAME, true)
                .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));

        assertAcked(client().admin().indices().prepareCreate("index1").get());
        assertAcked(client().admin().indices().prepareCreate("1index").get());
        ensureYellow();// wait for primaries to be allocated
        // Should succeed, since no wildcards
        assertAcked(client().admin().indices().prepareClose("1index").get());

        try {
            client().admin().indices().prepareClose("_all").get();
            fail();
        } catch (ElasticsearchIllegalArgumentException e) {}
        try {
            assertAcked(client().admin().indices().prepareOpen("_all").get());
            fail();
        } catch (ElasticsearchIllegalArgumentException e) {
        }
        try {
            client().admin().indices().prepareClose("*").get();
            fail();
        } catch (ElasticsearchIllegalArgumentException e) {}
        try {
            assertAcked(client().admin().indices().prepareOpen("*").get());
            fail();
        } catch (ElasticsearchIllegalArgumentException e) {
        }

        settings = ImmutableSettings.builder()
                .put(DestructiveOperations.REQUIRES_NAME, false)
                .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));
        assertAcked(client().admin().indices().prepareClose("_all").get());
        assertAcked(client().admin().indices().prepareOpen("_all").get());

        // end close index:
        client().admin().indices().prepareDelete("_all").get();
        // delete_by_query:
        settings = ImmutableSettings.builder()
                .put(DestructiveOperations.REQUIRES_NAME, true)
                .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));

        assertAcked(client().admin().indices().prepareCreate("index1").get());
        assertAcked(client().admin().indices().prepareCreate("1index").get());

        // Should succeed, since no wildcards
        client().prepareDeleteByQuery("1index").setQuery(QueryBuilders.matchAllQuery()).get();

        try {
            client().prepareDeleteByQuery("_all").setQuery(QueryBuilders.matchAllQuery()).get();
            fail();
        } catch (ElasticsearchIllegalArgumentException e) {}

        try {
            client().prepareDeleteByQuery().setQuery(QueryBuilders.matchAllQuery()).get();
            fail();
        } catch (ElasticsearchIllegalArgumentException e) {}

        settings = ImmutableSettings.builder()
                .put(DestructiveOperations.REQUIRES_NAME, false)
                .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));

        client().prepareDeleteByQuery().setQuery(QueryBuilders.matchAllQuery()).get();
        client().prepareDeleteByQuery("_all").setQuery(QueryBuilders.matchAllQuery()).get();

        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));
        client().prepareDeleteByQuery().setQuery(QueryBuilders.matchAllQuery()).get();
        // end delete_by_query:
        client().admin().indices().prepareDelete("_all").get();
    }

}
