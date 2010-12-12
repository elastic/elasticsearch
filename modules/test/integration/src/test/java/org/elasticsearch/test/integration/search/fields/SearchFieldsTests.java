/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration.search.fields;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentFactory;
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
public class SearchFieldsTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass public void createNodes() throws Exception {
        startNode("server1");
        startNode("server2");
        client = getClient();
    }

    @AfterClass public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("server1");
    }

    @Test public void testStoredFields() throws Exception {
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("field1").field("type", "string").field("store", "yes").endObject()
                .startObject("field2").field("type", "string").field("store", "no").endObject()
                .startObject("field3").field("type", "string").field("store", "yes").endObject()
                .endObject().endObject().endObject().string();

        client.admin().indices().preparePutMapping().setType("type1").setSource(mapping).execute().actionGet();

        Thread.sleep(100); // sleep a bit here..., so hte mappings get applied

        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("field1", "value1")
                .field("field2", "value2")
                .field("field3", "value3")
                .endObject()).execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch().setQuery(matchAllQuery()).addField("field1").execute().actionGet();
        assertThat(searchResponse.hits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.hits().hits().length, equalTo(1));
        assertThat(searchResponse.hits().getAt(0).fields().size(), equalTo(1));
        assertThat(searchResponse.hits().getAt(0).fields().get("field1").value().toString(), equalTo("value1"));

        // field2 is not stored, check that it gets extracted from source
        searchResponse = client.prepareSearch().setQuery(matchAllQuery()).addField("field2").execute().actionGet();
        assertThat(searchResponse.hits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.hits().hits().length, equalTo(1));
        assertThat(searchResponse.hits().getAt(0).fields().size(), equalTo(1));
        assertThat(searchResponse.hits().getAt(0).fields().get("field2").value().toString(), equalTo("value2"));

        searchResponse = client.prepareSearch().setQuery(matchAllQuery()).addField("field3").execute().actionGet();
        assertThat(searchResponse.hits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.hits().hits().length, equalTo(1));
        assertThat(searchResponse.hits().getAt(0).fields().size(), equalTo(1));
        assertThat(searchResponse.hits().getAt(0).fields().get("field3").value().toString(), equalTo("value3"));

        searchResponse = client.prepareSearch().setQuery(matchAllQuery()).addField("*").execute().actionGet();
        assertThat(searchResponse.hits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.hits().hits().length, equalTo(1));
        assertThat(searchResponse.hits().getAt(0).fields().size(), equalTo(2));
        assertThat(searchResponse.hits().getAt(0).fields().get("field1").value().toString(), equalTo("value1"));
        assertThat(searchResponse.hits().getAt(0).fields().get("field3").value().toString(), equalTo("value3"));
    }
}
