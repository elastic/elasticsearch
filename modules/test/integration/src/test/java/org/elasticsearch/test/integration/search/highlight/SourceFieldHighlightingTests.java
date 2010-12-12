/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.integration.search.highlight;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class SourceFieldHighlightingTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass public void createNodes() throws Exception {
        startNode("node1");
        client = getClient();
    }

    @AfterClass public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node1");
    }

    @Test public void testSourceLookupHighlighting() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        // we don't store title, now lets see if it works...
                        .startObject("title").field("type", "string").field("store", "no").field("term_vector", "with_positions_offsets").endObject()
                        .startObject("attachments").startObject("properties").startObject("body").field("type", "string").field("term_vector", "with_positions_offsets").endObject().endObject().endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        for (int i = 0; i < 5; i++) {
            client.prepareIndex("test", "type1", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .field("title", "This is a test on the highlighting bug present in elasticsearch")
                            .startArray("attachments").startObject().field("body", "attachment 1").endObject().startObject().field("body", "attachment 2").endObject().endArray()
                            .endObject())
                    .setRefresh(true).execute().actionGet();
        }

        SearchResponse search = client.prepareSearch()
                .setQuery(fieldQuery("title", "bug"))
                .addHighlightedField("title", -1, 0)
                .execute().actionGet();

        assertThat(search.hits().totalHits(), equalTo(5l));

        for (SearchHit hit : search.hits()) {
            assertThat(hit.highlightFields().get("title").fragments()[0], equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch"));
        }

        search = client.prepareSearch()
                .setQuery(fieldQuery("attachments.body", "attachment"))
                .addHighlightedField("attachments.body", -1, 0)
                .execute().actionGet();

        assertThat(search.hits().totalHits(), equalTo(5l));

        for (SearchHit hit : search.hits()) {
            assertThat(hit.highlightFields().get("attachments.body").fragments()[0], equalTo("<em>attachment</em> 1 <em>attachment</em> 2"));
        }
    }
}
