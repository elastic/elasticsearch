/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.template;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateV2Action;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateV2Action;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.metadata.IndexTemplateV2;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class TemplatePreferenceIT extends ESSingleNodeTestCase {

    private static final String INDEX = "index";

    @Before
    public void setup() throws Exception {
        assertAcked(client().admin().indices().preparePutTemplate("v1")
            .setSettings(Settings.builder()
                .put("index.priority", 15)
                .build())
            .setPatterns(Collections.singletonList(INDEX + "*")).get());

        Template v2Settings = new Template(Settings.builder()
            .put("index.priority", 23)
            .build(), null, null);
        IndexTemplateV2 v2template = new IndexTemplateV2(Collections.singletonList(INDEX + "*"), v2Settings, null, null, null, null);
        PutIndexTemplateV2Action.Request request = new PutIndexTemplateV2Action.Request("v2");
        request.indexTemplate(v2template);
        assertAcked(client().execute(PutIndexTemplateV2Action.INSTANCE, request).get());
    }

    @After
    public void cleanup() throws Exception {
        assertAcked(client().admin().indices().prepareDeleteTemplate("v1").get());
        assertAcked(client().execute(DeleteIndexTemplateV2Action.INSTANCE, new DeleteIndexTemplateV2Action.Request("v2")).get());
    }

    public void testCreateIndexPreference() throws Exception {
        client().admin().indices().prepareCreate(INDEX).get();
        assertUsedV2();

        client().admin().indices().create(new CreateIndexRequest(INDEX).preferV2Templates(false)).get();
        assertUsedV1();

        client().admin().indices().create(new CreateIndexRequest(INDEX).preferV2Templates(true)).get();
        assertUsedV2();
    }

    public void testIndexingRequestPreference() throws Exception {
        client().index(new IndexRequest(INDEX).source("foo", "bar")).get();
        assertUsedV2();

        client().index(new IndexRequest(INDEX).source("foo", "bar").preferV2Templates(false)).get();
        assertUsedV1();

        client().index(new IndexRequest(INDEX).source("foo", "bar").preferV2Templates(true)).get();
        assertUsedV2();

        client().update(new UpdateRequest(INDEX, "1").doc("foo", "bar").docAsUpsert(true)).get();
        assertUsedV2();

        client().update(new UpdateRequest(INDEX, "1").doc("foo", "bar").docAsUpsert(true).preferV2Templates(false)).get();
        assertUsedV1();

        client().update(new UpdateRequest(INDEX, "1").doc("foo", "bar").docAsUpsert(true).preferV2Templates(true)).get();
        assertUsedV2();

        client().bulk(new BulkRequest(INDEX).add(new IndexRequest(INDEX).source("foo", "bar"))).get();
        assertUsedV2();

        client().bulk(new BulkRequest(INDEX).add(new IndexRequest(INDEX).source("foo", "bar")).preferV2Templates(false)).get();
        assertUsedV1();

        client().bulk(new BulkRequest(INDEX).add(new IndexRequest(INDEX).source("foo", "bar")).preferV2Templates(true)).get();
        assertUsedV2();
    }

    public void testRolloverMaintainsSetting() throws Exception {
        {
            client().admin().indices().prepareCreate(INDEX + "-1")
                .addAlias(new Alias("alias").writeIndex(true))
                .get();

            client().admin().indices().prepareRolloverIndex("alias").get();
            GetSettingsResponse resp = client().admin().indices().prepareGetSettings().setIndices(INDEX + "-000002").get();
            assertThat("expected index to use V2 template and have priority of 23",
                resp.getSetting(INDEX + "-000002", "index.priority"), equalTo("23"));
            client().admin().indices().prepareDelete(INDEX + "*").get();
        }

        {
            client().admin().indices().prepareCreate(INDEX + "-1")
                .addAlias(new Alias("alias").writeIndex(true))
                .get();

            RolloverRequest request = new RolloverRequest("alias", INDEX + "-000002");
            request.getCreateIndexRequest().preferV2Templates(false);
            client().admin().indices().rolloverIndex(request).get();
            GetSettingsResponse resp = client().admin().indices().prepareGetSettings().setIndices(INDEX + "-000002").get();
            assertThat("expected index to use V1 template and have priority of 15",
                resp.getSetting(INDEX + "-000002", "index.priority"), equalTo("15"));
            client().admin().indices().prepareDelete(INDEX + "*").get();
        }

        {
            client().admin().indices().prepareCreate(INDEX + "-1")
                .addAlias(new Alias("alias").writeIndex(true))
                .get();

            RolloverRequest request = new RolloverRequest("alias", INDEX + "-000002");
            request.getCreateIndexRequest().preferV2Templates(true);
            client().admin().indices().rolloverIndex(request).get();
            GetSettingsResponse resp = client().admin().indices().prepareGetSettings().setIndices(INDEX + "-000002").get();
            assertThat("expected index to use V2 template and have priority of 23",
                resp.getSetting(INDEX + "-000002", "index.priority"), equalTo("23"));
            client().admin().indices().prepareDelete(INDEX + "*").get();
        }

        {
            client().admin().indices().create(new CreateIndexRequest(INDEX + "-1")
                .alias(new Alias("alias").writeIndex(true))
                .preferV2Templates(false))
                .get();

            client().admin().indices().prepareRolloverIndex("alias").get();
            GetSettingsResponse resp = client().admin().indices().prepareGetSettings().setIndices(INDEX + "-000002").get();
            assertThat("expected index to use V1 template and have priority of 15",
                resp.getSetting(INDEX + "-000002", "index.priority"), equalTo("15"));
            client().admin().indices().prepareDelete(INDEX + "*").get();
        }

        {
            client().admin().indices().create(new CreateIndexRequest(INDEX + "-1")
                .alias(new Alias("alias").writeIndex(true))
                .preferV2Templates(false))
                .get();

            RolloverRequest request = new RolloverRequest("alias", INDEX + "-000002");
            request.getCreateIndexRequest().preferV2Templates(false);
            client().admin().indices().rolloverIndex(request).get();
            GetSettingsResponse resp = client().admin().indices().prepareGetSettings().setIndices(INDEX + "-000002").get();
            assertThat("expected index to use V1 template and have priority of 15",
                resp.getSetting(INDEX + "-000002", "index.priority"), equalTo("15"));
            client().admin().indices().prepareDelete(INDEX + "*").get();
        }

        {
            client().admin().indices().create(new CreateIndexRequest(INDEX + "-1")
                .alias(new Alias("alias").writeIndex(true))
                .preferV2Templates(false))
                .get();

            RolloverRequest request = new RolloverRequest("alias", INDEX + "-000002");
            request.getCreateIndexRequest().preferV2Templates(true);
            client().admin().indices().rolloverIndex(request).get();
            GetSettingsResponse resp = client().admin().indices().prepareGetSettings().setIndices(INDEX + "-000002").get();
            assertThat("expected index to use V2 template and have priority of 23",
                resp.getSetting(INDEX + "-000002", "index.priority"), equalTo("23"));
            client().admin().indices().prepareDelete(INDEX + "*").get();
        }

        {
            client().admin().indices().create(new CreateIndexRequest(INDEX + "-1")
                .alias(new Alias("alias").writeIndex(true))
                .preferV2Templates(true))
                .get();

            client().admin().indices().prepareRolloverIndex("alias").get();
            GetSettingsResponse resp = client().admin().indices().prepareGetSettings().setIndices(INDEX + "-000002").get();
            assertThat("expected index to use V2 template and have priority of 23",
                resp.getSetting(INDEX + "-000002", "index.priority"), equalTo("23"));
            client().admin().indices().prepareDelete(INDEX + "*").get();
        }

        {
            client().admin().indices().create(new CreateIndexRequest(INDEX + "-1")
                .alias(new Alias("alias").writeIndex(true))
                .preferV2Templates(true))
                .get();

            RolloverRequest request = new RolloverRequest("alias", INDEX + "-000002");
            request.getCreateIndexRequest().preferV2Templates(false);
            client().admin().indices().rolloverIndex(request).get();
            GetSettingsResponse resp = client().admin().indices().prepareGetSettings().setIndices(INDEX + "-000002").get();
            assertThat("expected index to use V1 template and have priority of 15",
                resp.getSetting(INDEX + "-000002", "index.priority"), equalTo("15"));
            client().admin().indices().prepareDelete(INDEX + "*").get();
        }

        {
            client().admin().indices().create(new CreateIndexRequest(INDEX + "-1")
                .alias(new Alias("alias").writeIndex(true))
                .preferV2Templates(true))
                .get();

            RolloverRequest request = new RolloverRequest("alias", INDEX + "-000002");
            request.getCreateIndexRequest().preferV2Templates(true);
            client().admin().indices().rolloverIndex(request).get();
            GetSettingsResponse resp = client().admin().indices().prepareGetSettings().setIndices(INDEX + "-000002").get();
            assertThat("expected index to use V2 template and have priority of 23",
                resp.getSetting(INDEX + "-000002", "index.priority"), equalTo("23"));
            client().admin().indices().prepareDelete(INDEX + "*").get();
        }

        assertWarnings("index [index-000002] matches multiple v1 templates [one_shard_index_template, " +
            "random-soft-deletes-template, v1], v2 index templates will only match a single index template");
    }

    private void assertUsedV1() {
        GetSettingsResponse resp = client().admin().indices().prepareGetSettings().setIndices(INDEX).get();
        assertThat("expected index to use V1 template and have priority of 15",
            resp.getSetting(INDEX, "index.priority"), equalTo("15"));
        client().admin().indices().prepareDelete(INDEX).get();
    }

    private void assertUsedV2() {
        GetSettingsResponse resp = client().admin().indices().prepareGetSettings().setIndices(INDEX).get();
        assertThat("expected index to use V2 template and have priority of 23",
            resp.getSetting(INDEX, "index.priority"), equalTo("23"));
        client().admin().indices().prepareDelete(INDEX).get();
    }
}
