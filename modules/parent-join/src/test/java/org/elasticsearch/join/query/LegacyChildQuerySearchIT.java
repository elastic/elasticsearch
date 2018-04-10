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
package org.elasticsearch.join.query;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;
import static org.elasticsearch.join.query.JoinQueryBuilders.hasChildQuery;
import static org.elasticsearch.join.query.JoinQueryBuilders.hasParentQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public class LegacyChildQuerySearchIT extends ChildQuerySearchIT {

    @Override
    protected boolean legacy() {
        return true;
    }

    public void testIndexChildDocWithNoParentMapping() throws IOException {
        assertAcked(prepareCreate("test")
            .addMapping("parent")
            .addMapping("child1"));
        ensureGreen();

        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").get();
        try {
            client().prepareIndex("test", "child1", "c1").setParent("p1").setSource("c_field", "blue").get();
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.toString(), containsString("can't specify parent if no parent field has been configured"));
        }
        try {
            client().prepareIndex("test", "child2", "c2").setParent("p1").setSource("c_field", "blue").get();
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.toString(), containsString("can't specify parent if no parent field has been configured"));
        }

        refresh();
    }

    public void testAddingParentToExistingMapping() throws IOException {
        createIndex("test");
        ensureGreen();

        PutMappingResponse putMappingResponse = client().admin().indices()
            .preparePutMapping("test").setType("child").setSource("number", "type=integer")
            .get();
        assertThat(putMappingResponse.isAcknowledged(), equalTo(true));

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("test").get();
        Map<String, Object> mapping = getMappingsResponse.getMappings().get("test").get("child").getSourceAsMap();
        assertThat(mapping.size(), greaterThanOrEqualTo(1)); // there are potentially some meta fields configured randomly
        assertThat(mapping.get("properties"), notNullValue());

        try {
            // Adding _parent metadata field to existing mapping is prohibited:
            client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("child")
                .startObject("_parent").field("type", "parent").endObject()
                .endObject().endObject()).get();
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.toString(), containsString("The _parent field's type option can't be changed: [null]->[parent]"));
        }
    }

    // Issue #5783
    public void testQueryBeforeChildType() throws Exception {
        assertAcked(prepareCreate("test")
            .addMapping("features")
            .addMapping("posts", "_parent", "type=features")
            .addMapping("specials"));
        ensureGreen();

        client().prepareIndex("test", "features", "1").setSource("field", "foo").get();
        client().prepareIndex("test", "posts", "1").setParent("1").setSource("field", "bar").get();
        refresh();

        SearchResponse resp;
        resp = client().prepareSearch("test")
            .setSource(new SearchSourceBuilder().query(hasChildQuery("posts",
                QueryBuilders.matchQuery("field", "bar"), ScoreMode.None)))
            .get();
        assertHitCount(resp, 1L);
    }

    // Issue #6256
    public void testParentFieldInMultiMatchField() throws Exception {
        assertAcked(prepareCreate("test")
            .addMapping("type1")
            .addMapping("type2", "_parent", "type=type1")
        );
        ensureGreen();

        client().prepareIndex("test", "type2", "1").setParent("1").setSource("field", "value").get();
        refresh();

        SearchResponse response = client().prepareSearch("test")
            .setQuery(multiMatchQuery("1", "_parent#type1"))
            .get();

        assertThat(response.getHits().getTotalHits(), equalTo(1L));
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
    }

    public void testParentFieldToNonExistingType() {
        assertAcked(prepareCreate("test")
            .addMapping("parent").addMapping("child", "_parent", "type=parent2"));
        client().prepareIndex("test", "parent", "1").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("test", "child", "1").setParent("1").setSource("{}", XContentType.JSON).get();
        refresh();

        try {
            client().prepareSearch("test")
                .setQuery(hasChildQuery("child", matchAllQuery(), ScoreMode.None))
                .get();
            fail();
        } catch (SearchPhaseExecutionException e) {
        }
    }

    /*
   Test for https://github.com/elastic/elasticsearch/issues/3444
    */
    public void testBulkUpdateDocAsUpsertWithParent() throws Exception {
        assertAcked(prepareCreate("test")
            .addMapping("parent", "{\"parent\":{}}", XContentType.JSON)
            .addMapping("child", "{\"child\": {\"_parent\": {\"type\": \"parent\"}}}", XContentType.JSON));
        ensureGreen();

        BulkRequestBuilder builder = client().prepareBulk();

        // It's important to use JSON parsing here and request objects: issue 3444 is related to incomplete option parsing
        byte[] addParent = (
            "{" +
                "  \"index\" : {" +
                "    \"_index\" : \"test\"," +
                "    \"_type\"  : \"parent\"," +
                "    \"_id\"    : \"parent1\"" +
                "  }" +
                "}" +
                "\n" +
                "{" +
                "  \"field1\" : \"value1\"" +
                "}" +
                "\n").getBytes(StandardCharsets.UTF_8);

        byte[] addChild = (
            "{" +
                "  \"update\" : {" +
                "    \"_index\" : \"test\"," +
                "    \"_type\"  : \"child\"," +
                "    \"_id\"    : \"child1\"," +
                "    \"parent\" : \"parent1\"" +
                "  }" +
                "}" +
                "\n" +
                "{" +
                "  \"doc\" : {" +
                "    \"field1\" : \"value1\"" +
                "  }," +
                "  \"doc_as_upsert\" : \"true\"" +
                "}" +
                "\n").getBytes(StandardCharsets.UTF_8);

        builder.add(addParent, 0, addParent.length, XContentType.JSON);
        builder.add(addChild, 0, addChild.length, XContentType.JSON);

        BulkResponse bulkResponse = builder.get();
        assertThat(bulkResponse.getItems().length, equalTo(2));
        assertThat(bulkResponse.getItems()[0].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[1].isFailed(), equalTo(false));

        client().admin().indices().prepareRefresh("test").get();

        //we check that the _parent field was set on the child document by using the has parent query
        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(hasParentQuery("parent", QueryBuilders.matchAllQuery(), false))
            .get();

        assertNoFailures(searchResponse);
        assertSearchHits(searchResponse, "child1");
    }

    /*
    Test for https://github.com/elastic/elasticsearch/issues/3444
     */
    public void testBulkUpdateUpsertWithParent() throws Exception {
        assertAcked(prepareCreate("test")
            .addMapping("parent", "{\"parent\":{}}", XContentType.JSON)
            .addMapping("child", "{\"child\": {\"_parent\": {\"type\": \"parent\"}}}", XContentType.JSON));
        ensureGreen();

        BulkRequestBuilder builder = client().prepareBulk();

        byte[] addParent = (
            "{" +
                "  \"index\" : {" +
                "    \"_index\" : \"test\"," +
                "    \"_type\"  : \"parent\"," +
                "    \"_id\"    : \"parent1\"" +
                "  }" +
                "}" +
                "\n" +
                "{" +
                "  \"field1\" : \"value1\"" +
                "}" +
                "\n").getBytes(StandardCharsets.UTF_8);

        byte[] addChild1 = (
            "{" +
                "  \"update\" : {" +
                "    \"_index\" : \"test\"," +
                "    \"_type\"  : \"child\"," +
                "    \"_id\"    : \"child1\"," +
                "    \"parent\" : \"parent1\"" +
                "  }" +
                "}" +
                "\n" +
                "{" +
                "  \"script\" : {" +
                "    \"inline\" : \"ctx._source.field2 = 'value2'\"" +
                "  }," +
                "  \"lang\" : \"" + InnerHitsIT.CustomScriptPlugin.NAME + "\"," +
                "  \"upsert\" : {" +
                "    \"field1\" : \"value1'\"" +
                "  }" +
                "}" +
                "\n").getBytes(StandardCharsets.UTF_8);

        byte[] addChild2 = (
            "{" +
                "  \"update\" : {" +
                "    \"_index\" : \"test\"," +
                "    \"_type\"  : \"child\"," +
                "    \"_id\"    : \"child1\"," +
                "    \"parent\" : \"parent1\"" +
                "  }" +
                "}" +
                "\n" +
                "{" +
                "  \"script\" : \"ctx._source.field2 = 'value2'\"," +
                "  \"upsert\" : {" +
                "    \"field1\" : \"value1'\"" +
                "  }" +
                "}" +
                "\n").getBytes(StandardCharsets.UTF_8);

        builder.add(addParent, 0, addParent.length, XContentType.JSON);
        builder.add(addChild1, 0, addChild1.length, XContentType.JSON);
        builder.add(addChild2, 0, addChild2.length, XContentType.JSON);

        BulkResponse bulkResponse = builder.get();
        assertThat(bulkResponse.getItems().length, equalTo(3));
        assertThat(bulkResponse.getItems()[0].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[1].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[2].isFailed(), equalTo(true));
        assertThat(bulkResponse.getItems()[2].getFailure().getCause().getCause().getMessage(),
            equalTo("script_lang not supported [painless]"));

        client().admin().indices().prepareRefresh("test").get();

        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(hasParentQuery("parent", QueryBuilders.matchAllQuery(), false))
            .get();

        assertSearchHits(searchResponse, "child1");
    }

}
