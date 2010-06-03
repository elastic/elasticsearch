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

package org.elasticsearch.test.integration.search.facets;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.facets.MultiCountFacet;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.hamcrest.MatcherAssert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.elasticsearch.util.xcontent.XContentFactory.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class SimpleFacetsTests extends AbstractNodesTests {

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

    @Test public void testFieldFacets() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("stag", "111")
                .startArray("tag").value("xxx").value("yyy").endArray()
                .endObject()).execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("stag", "111")
                .startArray("tag").value("zzz").value("yyy").endArray()
                .endObject()).execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch()
                .setQuery(termQuery("stag", "111"))
                .addFieldFacet("facet1", "stag", 10)
                .addFieldFacet("facet2", "tag", 10)
                .execute().actionGet();

        MultiCountFacet<String> facet = (MultiCountFacet<String>) searchResponse.facets().facet("facet1");
        MatcherAssert.assertThat(facet.name(), equalTo("facet1"));
        MatcherAssert.assertThat(facet.entries().size(), equalTo(1));
        MatcherAssert.assertThat(facet.entries().get(0).value(), equalTo("111"));
        MatcherAssert.assertThat(facet.entries().get(0).count(), equalTo(2));

        facet = (MultiCountFacet<String>) searchResponse.facets().facet("facet2");
        MatcherAssert.assertThat(facet.name(), equalTo("facet2"));
        MatcherAssert.assertThat(facet.entries().size(), equalTo(3));
        MatcherAssert.assertThat(facet.entries().get(0).value(), equalTo("yyy"));
        MatcherAssert.assertThat(facet.entries().get(0).count(), equalTo(2));
    }
}
