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
package org.elasticsearch.indices.template;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.indices.IndexTemplateAlreadyExistsException;
import org.elasticsearch.indices.InvalidAliasNameException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleIndexTemplateTests extends ElasticsearchIntegrationTest {

    @Test
    public void simpleIndexTemplateTests() throws Exception {
        // clean all templates setup by the framework.
        client().admin().indices().prepareDeleteTemplate("*").get();

        // check get all templates on an empty index.
        GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates().get();
        assertThat(response.getIndexTemplates(), empty());


        client().admin().indices().preparePutTemplate("template_1")
                .setTemplate("te*")
                .setOrder(0)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field1").field("type", "string").field("store", "yes").endObject()
                        .startObject("field2").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                        .endObject().endObject().endObject())
                .get();

        client().admin().indices().preparePutTemplate("template_2")
                .setTemplate("test*")
                .setOrder(1)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field2").field("type", "string").field("store", "no").endObject()
                        .endObject().endObject().endObject())
                .get();

        // test create param
        assertThrows(client().admin().indices().preparePutTemplate("template_2")
                .setTemplate("test*")
                .setCreate(true)
                .setOrder(1)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field2").field("type", "string").field("store", "no").endObject()
                        .endObject().endObject().endObject())
                , IndexTemplateAlreadyExistsException.class
        );

        response = client().admin().indices().prepareGetTemplates().get();
        assertThat(response.getIndexTemplates(), hasSize(2));


        // index something into test_index, will match on both templates
        client().prepareIndex("test_index", "type1", "1").setSource("field1", "value1", "field2", "value 2").setRefresh(true).execute().actionGet();

        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test_index")
                .setQuery(termQuery("field1", "value1"))
                .addField("field1").addField("field2")
                .execute().actionGet();

        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getAt(0).field("field1").value().toString(), equalTo("value1"));
        assertThat(searchResponse.getHits().getAt(0).field("field2").value().toString(), equalTo("value 2")); // this will still be loaded because of the source feature

        client().prepareIndex("text_index", "type1", "1").setSource("field1", "value1", "field2", "value 2").setRefresh(true).execute().actionGet();

        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        // now only match on one template (template_1)
        searchResponse = client().prepareSearch("text_index")
                .setQuery(termQuery("field1", "value1"))
                .addField("field1").addField("field2")
                .execute().actionGet();
        if (searchResponse.getFailedShards() > 0) {
            logger.warn("failed search " + Arrays.toString(searchResponse.getShardFailures()));
        }
        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getAt(0).field("field1").value().toString(), equalTo("value1"));
        assertThat(searchResponse.getHits().getAt(0).field("field2").value().toString(), equalTo("value 2"));
    }

    @Test
    public void testDeleteIndexTemplate() throws Exception {
        final int existingTemplates = admin().cluster().prepareState().execute().actionGet().getState().metaData().templates().size();
        logger.info("--> put template_1 and template_2");
        client().admin().indices().preparePutTemplate("template_1")
                .setTemplate("te*")
                .setOrder(0)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field1").field("type", "string").field("store", "yes").endObject()
                        .startObject("field2").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        client().admin().indices().preparePutTemplate("template_2")
                .setTemplate("test*")
                .setOrder(1)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field2").field("type", "string").field("store", "no").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        logger.info("--> explicitly delete template_1");
        admin().indices().prepareDeleteTemplate("template_1").execute().actionGet();
        assertThat(admin().cluster().prepareState().execute().actionGet().getState().metaData().templates().size(), equalTo(1 + existingTemplates));
        assertThat(admin().cluster().prepareState().execute().actionGet().getState().metaData().templates().containsKey("template_2"), equalTo(true));
        assertThat(admin().cluster().prepareState().execute().actionGet().getState().metaData().templates().containsKey("template_1"), equalTo(false));


        logger.info("--> put template_1 back");
        client().admin().indices().preparePutTemplate("template_1")
                .setTemplate("te*")
                .setOrder(0)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field1").field("type", "string").field("store", "yes").endObject()
                        .startObject("field2").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        logger.info("--> delete template*");
        admin().indices().prepareDeleteTemplate("template*").execute().actionGet();
        assertThat(admin().cluster().prepareState().execute().actionGet().getState().metaData().templates().size(), equalTo(existingTemplates));

        logger.info("--> delete * with no templates, make sure we don't get a failure");
        admin().indices().prepareDeleteTemplate("*").execute().actionGet();
        assertThat(admin().cluster().prepareState().execute().actionGet().getState().metaData().templates().size(), equalTo(0));
    }

    @Test
    public void testThatGetIndexTemplatesWorks() throws Exception {
        logger.info("--> put template_1");
        client().admin().indices().preparePutTemplate("template_1")
                .setTemplate("te*")
                .setOrder(0)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field1").field("type", "string").field("store", "yes").endObject()
                        .startObject("field2").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        logger.info("--> get template template_1");
        GetIndexTemplatesResponse getTemplate1Response = client().admin().indices().prepareGetTemplates("template_1").execute().actionGet();
        assertThat(getTemplate1Response.getIndexTemplates(), hasSize(1));
        assertThat(getTemplate1Response.getIndexTemplates().get(0), is(notNullValue()));
        assertThat(getTemplate1Response.getIndexTemplates().get(0).getTemplate(), is("te*"));
        assertThat(getTemplate1Response.getIndexTemplates().get(0).getOrder(), is(0));

        logger.info("--> get non-existing-template");
        GetIndexTemplatesResponse getTemplate2Response = client().admin().indices().prepareGetTemplates("non-existing-template").execute().actionGet();
        assertThat(getTemplate2Response.getIndexTemplates(), hasSize(0));
    }

    @Test
    public void testThatGetIndexTemplatesWithSimpleRegexWorks() throws Exception {
        logger.info("--> put template_1");
        client().admin().indices().preparePutTemplate("template_1")
                .setTemplate("te*")
                .setOrder(0)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field1").field("type", "string").field("store", "yes").endObject()
                        .startObject("field2").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        logger.info("--> put template_2");
        client().admin().indices().preparePutTemplate("template_2")
                .setTemplate("te*")
                .setOrder(0)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field1").field("type", "string").field("store", "yes").endObject()
                        .startObject("field2").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        logger.info("--> put template3");
        client().admin().indices().preparePutTemplate("template3")
                .setTemplate("te*")
                .setOrder(0)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field1").field("type", "string").field("store", "yes").endObject()
                        .startObject("field2").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        logger.info("--> get template template_*");
        GetIndexTemplatesResponse getTemplate1Response = client().admin().indices().prepareGetTemplates("template_*").execute().actionGet();
        assertThat(getTemplate1Response.getIndexTemplates(), hasSize(2));

        List<String> templateNames = Lists.newArrayList();
        templateNames.add(getTemplate1Response.getIndexTemplates().get(0).name());
        templateNames.add(getTemplate1Response.getIndexTemplates().get(1).name());
        assertThat(templateNames, containsInAnyOrder("template_1", "template_2"));

        logger.info("--> get all templates");
        getTemplate1Response = client().admin().indices().prepareGetTemplates("template*").execute().actionGet();
        assertThat(getTemplate1Response.getIndexTemplates(), hasSize(3));

        templateNames = Lists.newArrayList();
        templateNames.add(getTemplate1Response.getIndexTemplates().get(0).name());
        templateNames.add(getTemplate1Response.getIndexTemplates().get(1).name());
        templateNames.add(getTemplate1Response.getIndexTemplates().get(2).name());
        assertThat(templateNames, containsInAnyOrder("template_1", "template_2", "template3"));

        logger.info("--> get templates template_1 and template_2");
        getTemplate1Response = client().admin().indices().prepareGetTemplates("template_1", "template_2").execute().actionGet();
        assertThat(getTemplate1Response.getIndexTemplates(), hasSize(2));

        templateNames = Lists.newArrayList();
        templateNames.add(getTemplate1Response.getIndexTemplates().get(0).name());
        templateNames.add(getTemplate1Response.getIndexTemplates().get(1).name());
        assertThat(templateNames, containsInAnyOrder("template_1", "template_2"));
    }

    @Test
    public void testThatInvalidGetIndexTemplatesFails() throws Exception {
        logger.info("--> get template null");
        testExpectActionRequestValidationException(null);

        logger.info("--> get template empty");
        testExpectActionRequestValidationException("");

        logger.info("--> get template 'a', '', 'c'");
        testExpectActionRequestValidationException("a", "", "c");

        logger.info("--> get template 'a', null, 'c'");
        testExpectActionRequestValidationException("a", null, "c");
    }

    private void testExpectActionRequestValidationException(String... names) {
        assertThrows(client().admin().indices().prepareGetTemplates(names),
                ActionRequestValidationException.class,
                "get template with " + Arrays.toString(names));
    }

    @Test
    public void testBrokenMapping() throws Exception {
        // clean all templates setup by the framework.
        client().admin().indices().prepareDeleteTemplate("*").get();

        // check get all templates on an empty index.
        GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates().get();
        assertThat(response.getIndexTemplates(), empty());

        client().admin().indices().preparePutTemplate("template_1")
                .setTemplate("te*")
                .addMapping("type1", "abcde")
                .get();

        response = client().admin().indices().prepareGetTemplates().get();
        assertThat(response.getIndexTemplates(), hasSize(1));
        assertThat(response.getIndexTemplates().get(0).getMappings().size(), equalTo(1));
        assertThat(response.getIndexTemplates().get(0).getMappings().get("type1").string(), equalTo("abcde"));

        try {
            createIndex("test");
            fail("create index should have failed due to broken index templates mapping");
        } catch(ElasticsearchParseException e) {
            //everything fine
        }
    }

    @Test
    public void testInvalidSettings() throws Exception {
        // clean all templates setup by the framework.
        client().admin().indices().prepareDeleteTemplate("*").get();

        // check get all templates on an empty index.
        GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates().get();
        assertThat(response.getIndexTemplates(), empty());

        client().admin().indices().preparePutTemplate("template_1")
                .setTemplate("te*")
                .setSettings(ImmutableSettings.builder().put("does_not_exist", "test"))
                .get();

        response = client().admin().indices().prepareGetTemplates().get();
        assertThat(response.getIndexTemplates(), hasSize(1));
        assertThat(response.getIndexTemplates().get(0).getSettings().getAsMap().size(), equalTo(1));
        assertThat(response.getIndexTemplates().get(0).getSettings().get("index.does_not_exist"), equalTo("test"));

        createIndex("test");

        //the wrong setting has no effect but does get stored among the index settings
        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings("test").get();
        assertThat(getSettingsResponse.getIndexToSettings().get("test").getAsMap().get("index.does_not_exist"), equalTo("test"));
    }

    public void testIndexTemplateWithAliases() throws Exception {

        client().admin().indices().preparePutTemplate("template_with_aliases")
                .setTemplate("te*")
                .addMapping("type1", "{\"type1\" : {\"properties\" : {\"value\" : {\"type\" : \"string\"}}}}")
                .addAlias(new Alias("simple_alias"))
                .addAlias(new Alias("templated_alias-{index}"))
                .addAlias(new Alias("filtered_alias").filter("{\"type\":{\"value\":\"type2\"}}"))
                .addAlias(new Alias("complex_filtered_alias")
                        .filter(FilterBuilders.termsFilter("_type",  "typeX", "typeY", "typeZ").execution("bool").cache(true)))
                .get();

        assertAcked(prepareCreate("test_index").addMapping("type1").addMapping("type2").addMapping("typeX").addMapping("typeY").addMapping("typeZ"));
        ensureGreen();

        client().prepareIndex("test_index", "type1", "1").setSource("field", "A value").get();
        client().prepareIndex("test_index", "type2", "2").setSource("field", "B value").get();
        client().prepareIndex("test_index", "typeX", "3").setSource("field", "C value").get();
        client().prepareIndex("test_index", "typeY", "4").setSource("field", "D value").get();
        client().prepareIndex("test_index", "typeZ", "5").setSource("field", "E value").get();

        GetAliasesResponse getAliasesResponse = client().admin().indices().prepareGetAliases().setIndices("test_index").get();
        assertThat(getAliasesResponse.getAliases().size(), equalTo(1));
        assertThat(getAliasesResponse.getAliases().get("test_index").size(), equalTo(4));

        refresh();

        SearchResponse searchResponse = client().prepareSearch("test_index").get();
        assertHitCount(searchResponse, 5l);

        searchResponse = client().prepareSearch("simple_alias").get();
        assertHitCount(searchResponse, 5l);

        searchResponse = client().prepareSearch("templated_alias-test_index").get();
        assertHitCount(searchResponse, 5l);

        searchResponse = client().prepareSearch("filtered_alias").get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).type(), equalTo("type2"));

        // Search the complex filter alias
        searchResponse = client().prepareSearch("complex_filtered_alias").get();
        assertHitCount(searchResponse, 3l);

        Set<String> types = Sets.newHashSet();
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            types.add(searchHit.getType());
        }
        assertThat(types.size(), equalTo(3));
        assertThat(types, containsInAnyOrder("typeX", "typeY", "typeZ"));
    }

    @Test
    public void testIndexTemplateWithAliasesInSource() {
        client().admin().indices().preparePutTemplate("template_1")
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
                        "}").get();


        assertAcked(prepareCreate("test_index").addMapping("type1").addMapping("type2"));
        ensureGreen();

        GetAliasesResponse getAliasesResponse = client().admin().indices().prepareGetAliases().setIndices("test_index").get();
        assertThat(getAliasesResponse.getAliases().size(), equalTo(1));
        assertThat(getAliasesResponse.getAliases().get("test_index").size(), equalTo(1));

        client().prepareIndex("test_index", "type1", "1").setSource("field", "value1").get();
        client().prepareIndex("test_index", "type2", "2").setSource("field", "value2").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test_index").get();
        assertHitCount(searchResponse, 2l);

        searchResponse = client().prepareSearch("my_alias").get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).type(), equalTo("type2"));
    }

    @Test
    public void testIndexTemplateWithAliasesSource() {
        client().admin().indices().preparePutTemplate("template_1")
                .setTemplate("te*")
                .setAliases(
                        "    {\n" +
                        "        \"alias1\" : {},\n" +
                        "        \"alias2\" : {\n" +
                        "            \"filter\" : {\n" +
                        "                \"type\" : {\n" +
                        "                    \"value\" : \"type2\"\n" +
                        "                }\n" +
                        "            }\n" +
                        "         },\n" +
                        "        \"alias3\" : { \"routing\" : \"1\" }" +
                        "    }\n").get();

        assertAcked(prepareCreate("test_index").addMapping("type1").addMapping("type2"));
        ensureGreen();

        GetAliasesResponse getAliasesResponse = client().admin().indices().prepareGetAliases().setIndices("test_index").get();
        assertThat(getAliasesResponse.getAliases().size(), equalTo(1));
        assertThat(getAliasesResponse.getAliases().get("test_index").size(), equalTo(3));

        client().prepareIndex("test_index", "type1", "1").setSource("field", "value1").get();
        client().prepareIndex("test_index", "type2", "2").setSource("field", "value2").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test_index").get();
        assertHitCount(searchResponse, 2l);

        searchResponse = client().prepareSearch("alias1").get();
        assertHitCount(searchResponse, 2l);

        searchResponse = client().prepareSearch("alias2").get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).type(), equalTo("type2"));
    }

    @Test
    public void testDuplicateAlias() throws Exception {
        client().admin().indices().preparePutTemplate("template_1")
                .setTemplate("te*")
                .addAlias(new Alias("my_alias").filter(termFilter("field", "value1")))
                .addAlias(new Alias("my_alias").filter(termFilter("field", "value2")))
                .get();

        GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates("template_1").get();
        assertThat(response.getIndexTemplates().size(), equalTo(1));
        assertThat(response.getIndexTemplates().get(0).getAliases().size(), equalTo(1));
        assertThat(response.getIndexTemplates().get(0).getAliases().get("my_alias").filter().string(), containsString("\"value1\""));
    }

    @Test
    public void testAliasInvalidFilterValidJson() throws Exception {

        //invalid filter but valid json: put index template works fine, fails during index creation
        client().admin().indices().preparePutTemplate("template_1")
                .setTemplate("te*")
                .addAlias(new Alias("invalid_alias").filter("{ \"invalid\": {} }")).get();

        GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates("template_1").get();
        assertThat(response.getIndexTemplates().size(), equalTo(1));
        assertThat(response.getIndexTemplates().get(0).getAliases().size(), equalTo(1));
        assertThat(response.getIndexTemplates().get(0).getAliases().get("invalid_alias").filter().string(), equalTo("{\"invalid\":{}}"));

        try {
            createIndex("test");
            fail("index creation should have failed due to invalid alias filter in matching index template");
        } catch(ElasticsearchIllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("failed to parse filter for alias [invalid_alias]"));
            assertThat(e.getCause(), instanceOf(QueryParsingException.class));
            assertThat(e.getCause().getMessage(), equalTo("No filter registered for [invalid]"));
        }
    }

    @Test
    public void testAliasInvalidFilterInvalidJson() throws Exception {

        //invalid json: put index template fails
        PutIndexTemplateRequestBuilder putIndexTemplateRequestBuilder = client().admin().indices().preparePutTemplate("template_1")
                .setTemplate("te*")
                .addAlias(new Alias("invalid_alias").filter("abcde"));

        try {
            putIndexTemplateRequestBuilder.get();
        } catch(ElasticsearchIllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("failed to parse filter for alias [invalid_alias]"));
        }

        GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates("template_1").get();
        assertThat(response.getIndexTemplates().size(), equalTo(0));
    }

    @Test
    public void testAliasNameExistingIndex() throws Exception {

        createIndex("index");

        client().admin().indices().preparePutTemplate("template_1")
                .setTemplate("te*")
                .addAlias(new Alias("index")).get();

        try {
            createIndex("test");
            fail("index creation should have failed due to alias with existing index name in mathching index template");
        } catch(InvalidAliasNameException e) {
            assertThat(e.getMessage(), equalTo("Invalid alias name [index], an index exists with the same name as the alias"));
        }
    }

    @Test
    public void testAliasEmptyName() throws Exception {
        PutIndexTemplateRequestBuilder putIndexTemplateRequestBuilder = client().admin().indices().preparePutTemplate("template_1")
                .setTemplate("te*")
                .addAlias(new Alias("  ").indexRouting("1,2,3"));

        try {
            putIndexTemplateRequestBuilder.get();
            fail("put template should have failed due to alias with empty name");
        } catch (ElasticsearchIllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("alias name is required"));
        }
    }

    @Test
    public void testAliasWithMultipleIndexRoutings() throws Exception {
        PutIndexTemplateRequestBuilder putIndexTemplateRequestBuilder = client().admin().indices().preparePutTemplate("template_1")
                .setTemplate("te*")
                .addAlias(new Alias("alias").indexRouting("1,2,3"));

        try {
            putIndexTemplateRequestBuilder.get();
            fail("put template should have failed due to alias with multiple index routings");
        } catch (ElasticsearchIllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("alias [alias] has several index routing values associated with it"));
        }
    }

    @Test
    public void testMultipleAliasesPrecedence() throws Exception {
        client().admin().indices().preparePutTemplate("template1")
                .setTemplate("*")
                .setOrder(0)
                .addAlias(new Alias("alias1"))
                .addAlias(new Alias("{index}-alias"))
                .addAlias(new Alias("alias3").filter(FilterBuilders.missingFilter("test")))
                .addAlias(new Alias("alias4")).get();

        client().admin().indices().preparePutTemplate("template2")
                .setTemplate("te*")
                .setOrder(1)
                .addAlias(new Alias("alias1").routing("test"))
                .addAlias(new Alias("alias3")).get();


        assertAcked(prepareCreate("test").addAlias(new Alias("test-alias").searchRouting("test-routing")));

        ensureGreen();

        GetAliasesResponse getAliasesResponse = client().admin().indices().prepareGetAliases().addIndices("test").get();
        assertThat(getAliasesResponse.getAliases().get("test").size(), equalTo(4));

        for (AliasMetaData aliasMetaData : getAliasesResponse.getAliases().get("test")) {
            assertThat(aliasMetaData.alias(), anyOf(equalTo("alias1"), equalTo("test-alias"), equalTo("alias3"), equalTo("alias4")));
            if ("alias1".equals(aliasMetaData.alias())) {
                assertThat(aliasMetaData.indexRouting(), equalTo("test"));
                assertThat(aliasMetaData.searchRouting(), equalTo("test"));
            } else if ("alias3".equals(aliasMetaData.alias())) {
                assertThat(aliasMetaData.filter(), nullValue());
            } else if ("test-alias".equals(aliasMetaData.alias())) {
                assertThat(aliasMetaData.indexRouting(), nullValue());
                assertThat(aliasMetaData.searchRouting(), equalTo("test-routing"));
            }
        }
    }

    @Test
    public void testStrictAliasParsingInIndicesCreatedViaTemplates() throws Exception {
        // Indexing into a should succeed, because the field mapping for field 'field' is defined in the test mapping.
        client().admin().indices().preparePutTemplate("template1")
                .setTemplate("a*")
                .setOrder(0)
                .addMapping("test", "field", "type=string")
                .addAlias(new Alias("alias1").filter(termFilter("field", "value"))).get();
        // Indexing into b should succeed, because the field mapping for field 'field' is defined in the _default_ mapping and the test type exists.
        client().admin().indices().preparePutTemplate("template2")
                .setTemplate("b*")
                .setOrder(0)
                .addMapping("_default_", "field", "type=string")
                .addMapping("test")
                .addAlias(new Alias("alias2").filter(termFilter("field", "value"))).get();
        // Indexing into c should succeed, because the field mapping for field 'field' is defined in the _default_ mapping.
        client().admin().indices().preparePutTemplate("template3")
                .setTemplate("c*")
                .setOrder(0)
                .addMapping("_default_", "field", "type=string")
                .addAlias(new Alias("alias3").filter(termFilter("field", "value"))).get();
        // Indexing into d index should fail, since there is field with name 'field' in the mapping
        client().admin().indices().preparePutTemplate("template4")
                .setTemplate("d*")
                .setOrder(0)
                .addAlias(new Alias("alias4").filter(termFilter("field", "value"))).get();

        client().prepareIndex("a1", "test", "test").setSource("{}").get();
        BulkResponse response = client().prepareBulk().add(new IndexRequest("a2", "test", "test").source("{}")).get();
        assertThat(response.hasFailures(), is(false));
        assertThat(response.getItems()[0].isFailed(), equalTo(false));
        assertThat(response.getItems()[0].getIndex(), equalTo("a2"));
        assertThat(response.getItems()[0].getType(), equalTo("test"));
        assertThat(response.getItems()[0].getId(), equalTo("test"));
        assertThat(response.getItems()[0].getVersion(), equalTo(1l));

        client().prepareIndex("b1", "test", "test").setSource("{}").get();
        response = client().prepareBulk().add(new IndexRequest("b2", "test", "test").source("{}")).get();
        assertThat(response.hasFailures(), is(false));
        assertThat(response.getItems()[0].isFailed(), equalTo(false));
        assertThat(response.getItems()[0].getIndex(), equalTo("b2"));
        assertThat(response.getItems()[0].getType(), equalTo("test"));
        assertThat(response.getItems()[0].getId(), equalTo("test"));
        assertThat(response.getItems()[0].getVersion(), equalTo(1l));

        client().prepareIndex("c1", "test", "test").setSource("{}").get();
        response = client().prepareBulk().add(new IndexRequest("c2", "test", "test").source("{}")).get();
        assertThat(response.hasFailures(), is(false));
        assertThat(response.getItems()[0].isFailed(), equalTo(false));
        assertThat(response.getItems()[0].getIndex(), equalTo("c2"));
        assertThat(response.getItems()[0].getType(), equalTo("test"));
        assertThat(response.getItems()[0].getId(), equalTo("test"));
        assertThat(response.getItems()[0].getVersion(), equalTo(1l));

        try {
            client().prepareIndex("d1", "test", "test").setSource("{}").get();
            fail();
        } catch (Exception e) {
            assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(ElasticsearchIllegalArgumentException.class));
            assertThat(e.getMessage(), containsString("failed to parse filter for alias [alias4]"));
        }
        response = client().prepareBulk().add(new IndexRequest("d2", "test", "test").source("{}")).get();
        assertThat(response.hasFailures(), is(true));
        assertThat(response.getItems()[0].isFailed(), equalTo(true));
        assertThat(response.getItems()[0].getFailureMessage(), containsString("failed to parse filter for alias [alias4]"));
    }
}
