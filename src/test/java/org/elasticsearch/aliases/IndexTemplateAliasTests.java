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

package org.elasticsearch.aliases;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.cluster.metadata.AliasMetaData.newAliasMetaDataBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
// FIXME: jbrook - Documentation for aliases in index templates
// FIXME: jbrook - Move to another package - not an integration test
// FIXME: jbrook - Test for index templates included via config file?
public class IndexTemplateAliasTests extends ElasticsearchIntegrationTest {

    private static final String MAPPING_SOURCE1 = "{\"mapping1\":{\"text1\":{\"type\":\"string\"}}}";
    private static final String MAPPING_SOURCE2 = "{\"mapping2\":{\"text2\":{\"type\":\"string\"}}}";
    
    @Test
    public void indexTemplateAliasTest() throws Exception {
        //clean();
        
        admin().indices().preparePutTemplate("template_with_aliases")
        .setTemplate("te*")
        .addAlias(newAliasMetaDataBuilder("simple_alias").build())
        .addAlias(newAliasMetaDataBuilder("templated_alias-{index}").build())
        .addAlias(newAliasMetaDataBuilder("filter_alias")
        		.filter("{\"type\":{\"value\":\"type2\"}}")
        		.build())
        .addAlias(newAliasMetaDataBuilder("complex_filter_alias")
        		.filter(XContentFactory.jsonBuilder().startObject()
        				.startObject("terms")
        					.array("_type", "typeX", "typeY", "typeZ")
        					.field("execution").value("bool")
        					.field("_cache").value(true)
        				.endObject()
       			) 
    			.build())			
    	.execute().actionGet();

        // index something into test_index, will match on template
        client().index(indexRequest("test_index").type("type1").id("1").source("field", "A value")).actionGet();
        client().index(indexRequest("test_index").type("type2").id("2").source("field", "B value")).actionGet();
        
        client().index(indexRequest("test_index").type("typeX").id("3").source("field", "C value")).actionGet();
        client().index(indexRequest("test_index").type("typeY").id("4").source("field", "D value")).actionGet();
        client().index(indexRequest("test_index").type("typeZ").id("5").source("field", "E value")).actionGet();
        
        admin().indices().prepareRefresh("test_index").execute().actionGet();
        
        admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        
        AliasMetaData filterAliasMetaData = cluster().clusterService().state().metaData().aliases().get("filter_alias").get("test_index");
        assertThat(filterAliasMetaData.alias(), equalTo("filter_alias"));
        assertThat(filterAliasMetaData.filter(), notNullValue(CompressedString.class));
        
        AliasMetaData complexFilterAliasMetaData = cluster().clusterService().state().metaData().aliases().get("complex_filter_alias").get("test_index");
        // Search the simple alias
        SearchResponse searchResponse = client().prepareSearch("simple_alias")
                .setQuery(QueryBuilders.matchAllQuery())
                .execute().actionGet();
        ElasticsearchAssertions.assertHitCount(searchResponse, 5l);
        
        // Search the templated alias
        searchResponse = client().prepareSearch("templated_alias-test_index")
                .setQuery(QueryBuilders.matchAllQuery())
                .execute().actionGet();
        ElasticsearchAssertions.assertHitCount(searchResponse, 5l);
        
        // Search the filter alias expecting only one result of "type2"
        searchResponse = client().prepareSearch("filter_alias")
                .setQuery(QueryBuilders.matchAllQuery())
                .execute().actionGet();
        ElasticsearchAssertions.assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).type(), equalTo("type2"));
        
        // Search the complex filter alias
        searchResponse = client().prepareSearch("complex_filter_alias")
                .setQuery(QueryBuilders.matchAllQuery())
                .execute().actionGet();
        ElasticsearchAssertions.assertHitCount(searchResponse, 3l);
    }
        
    @Test
    public void indexTemplateAliasUsingSourceTest() {
        admin().indices().preparePutTemplate("template_1")
                .setSource("{\n" +
                        "    \"template\" : \"*\",\n" +
                        "    \"aliases\" : {\n" +
                        "        \"my_alias\" : {\n" +
                        "            \"filter\" : {\n" +
                        "                \"type\" : {\n" +
                        "                    \"value\" : \"type2\"\n" +
                        "                }\n" +
                        "            }\n" +
                        "        }\n" +
                        "    }\n" +
                        "}")
                .execute().actionGet();
        
        admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1))
                .execute().actionGet();
        ensureGreen();

        ClusterState clusterState = admin().cluster().prepareState().execute().actionGet().getState();
        IndexTemplateMetaData templateMetaData = clusterState.metaData().templates().get("template_1");
        AliasMetaData aliasMetaData = clusterState.metaData().index("test").aliases().get("my_alias");
        assertThat(templateMetaData, Matchers.notNullValue());
        assertThat(aliasMetaData, Matchers.notNullValue());

        client().prepareIndex("test", "type1", "1").setSource("field", "value1").setRefresh(true).execute().actionGet();
        client().prepareIndex("test", "type2", "2").setSource("field", "value2").setRefresh(true).execute().actionGet();
        
        SearchResponse searchResponse = client().prepareSearch("my_alias")
                .setQuery(QueryBuilders.matchAllQuery())
                .execute().actionGet();
        ElasticsearchAssertions.assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).type(), equalTo("type2"));
    }
    
    @Test
    public void duplicateAliasTest() {
        admin().indices().preparePutTemplate("template_1")
        .setTemplate("te*")
        .addAlias(
                newAliasMetaDataBuilder("my_alias")
                .filter(FilterBuilders.termFilter("field", "value").toString())
                .build())
        .addAlias(
                newAliasMetaDataBuilder("my_alias")
                .filter(FilterBuilders.termFilter("field", "a_different_value").toString())
                .build())
        .execute().actionGet();
        
        admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1))
                .execute().actionGet();
        ensureGreen();
        
        ClusterState clusterState = admin().cluster().prepareState().execute().actionGet().getState();
        AliasMetaData aliasMetaData = clusterState.metaData().index("test").aliases().get("my_alias");
        assertEquals(clusterState.metaData().index("test").aliases().size(), 1);
        
        // It should take the last one
        assertTrue(aliasMetaData.filter().toString().contains("a_different_value"));
    }
    
    @Test
    public void aliasWithBrokenFilterTest() {
        try {
            admin().indices().preparePutTemplate("template_1")
            .setTemplate("te*")
            .addAlias(
                    newAliasMetaDataBuilder("my_alias")
                    .filter("{ t }")
                    .build())
            .execute().actionGet();
            
            assert false;
        } catch (Exception e) {
            // all is well
        }
    }

}
