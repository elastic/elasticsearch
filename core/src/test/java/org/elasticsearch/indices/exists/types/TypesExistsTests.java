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
package org.elasticsearch.indices.exists.types;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.equalTo;

public class TypesExistsTests extends ElasticsearchIntegrationTest {

    @Test
    public void testSimple() throws Exception {
        Client client = client();
        client.admin().indices().prepareCreate("test1")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").endObject().endObject())
                .addMapping("type2", jsonBuilder().startObject().startObject("type2").endObject().endObject())
                .execute().actionGet();
        client.admin().indices().prepareCreate("test2")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").endObject().endObject())
                .execute().actionGet();
        client.admin().indices().prepareAliases().addAlias("test1", "alias1").execute().actionGet();
        ClusterHealthResponse healthResponse = client.admin().cluster()
                .prepareHealth("test1", "test2").setWaitForYellowStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        TypesExistsResponse response = client.admin().indices().prepareTypesExists("test1").setTypes("type1").execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        response = client.admin().indices().prepareTypesExists("test1").setTypes("type2").execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        response = client.admin().indices().prepareTypesExists("test1").setTypes("type3").execute().actionGet();
        assertThat(response.isExists(), equalTo(false));
        try {
            client.admin().indices().prepareTypesExists("notExist").setTypes("type1").execute().actionGet();
            fail("Exception should have been thrown");
        } catch (IndexMissingException e) {}
        try {
            client.admin().indices().prepareTypesExists("notExist").setTypes("type0").execute().actionGet();
            fail("Exception should have been thrown");
        } catch (IndexMissingException e) {}
        response = client.admin().indices().prepareTypesExists("alias1").setTypes("type1").execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        response = client.admin().indices().prepareTypesExists("*").setTypes("type1").execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        response = client.admin().indices().prepareTypesExists("test1", "test2").setTypes("type1").execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        response = client.admin().indices().prepareTypesExists("test1", "test2").setTypes("type2").execute().actionGet();
        assertThat(response.isExists(), equalTo(false));
    }

    @Test
    public void testTypesExistsWithBlocks() throws IOException {
        assertAcked(prepareCreate("ro").addMapping("type1", jsonBuilder().startObject().startObject("type1").endObject().endObject()));
        ensureGreen("ro");

        // Request is not blocked
        for (String block : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY)) {
            try {
                enableIndexBlock("ro", block);
                assertThat(client().admin().indices().prepareTypesExists("ro").setTypes("type1").execute().actionGet().isExists(), equalTo(true));
            } finally {
                disableIndexBlock("ro", block);
            }
        }

        // Request is blocked
        try {
            enableIndexBlock("ro", IndexMetaData.SETTING_BLOCKS_METADATA);
            assertBlocked(client().admin().indices().prepareTypesExists("ro").setTypes("type1"));
        } finally {
            disableIndexBlock("ro", IndexMetaData.SETTING_BLOCKS_METADATA);
        }
    }
}
