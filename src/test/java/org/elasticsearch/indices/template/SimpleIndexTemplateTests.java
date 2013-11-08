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

package org.elasticsearch.indices.template;

import com.google.common.collect.Lists;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.indices.IndexTemplateAlreadyExistsException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
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

}
