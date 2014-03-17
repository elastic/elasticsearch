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
package org.elasticsearch.index.query;

import com.google.common.collect.Maps;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.is;

/**
 * Full integration test of the template query plugin.
 * */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class TemplateQueryTest extends ElasticsearchIntegrationTest {

    @Before
    public void setup() throws IOException {
        createIndex("test");
        ensureGreen("test");

        index("test", "testtype", "1", jsonBuilder().startObject().field("text", "value1").endObject());
        index("test", "testtype", "2", jsonBuilder().startObject().field("text", "value2").endObject());
        refresh();
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        String scriptPath = this.getClass().getResource("config").getPath();
        return settingsBuilder().put("path.conf", scriptPath).build();
    }

    @Test
    public void testTemplateInBody() throws IOException {
        Map<String, Object> vars = new HashMap<String, Object>();
        vars.put("template", "all");

        TemplateQueryBuilder builder = new TemplateQueryBuilder(
                "{\"match_{{template}}\": {}}\"", vars);
        SearchResponse sr = client().prepareSearch().setQuery(builder)
                .execute().actionGet();
        assertHitCount(sr, 2);
    }

    @Test
    public void testTemplateWOReplacementInBody() throws IOException {
        Map<String, Object> vars = new HashMap<String, Object>();

        TemplateQueryBuilder builder = new TemplateQueryBuilder(
                "{\"match_all\": {}}\"", vars);
        SearchResponse sr = client().prepareSearch().setQuery(builder)
                .execute().actionGet();
        assertHitCount(sr, 2);
    }

    @Test
    public void testTemplateInFile() {
        Map<String, Object> vars = new HashMap<String, Object>();
        vars.put("template", "all");

        TemplateQueryBuilder builder = new TemplateQueryBuilder(
                "storedTemplate", vars);
        SearchResponse sr = client().prepareSearch().setQuery(builder)
                .execute().actionGet();
        assertHitCount(sr, 2);

    }

    @Test
    public void testRawEscapedTemplate() throws IOException {
        String query = "{\"template\": {\"query\": \"{\\\"match_{{template}}\\\": {}}\\\"\",\"params\" : {\"template\" : \"all\"}}}";

        SearchResponse sr = client().prepareSearch().setQuery(query).get();
        assertHitCount(sr, 2);
    }

    @Test
    public void testRawTemplate() throws IOException {
        String query = "{\"template\": {\"query\": {\"match_{{template}}\": {}},\"params\" : {\"template\" : \"all\"}}}";
        SearchResponse sr = client().prepareSearch().setQuery(query).get();
        assertHitCount(sr, 2);
    }

    @Test
    public void testRawFSTemplate() throws IOException {
        String query = "{\"template\": {\"query\": \"storedTemplate\",\"params\" : {\"template\" : \"all\"}}}";

        SearchResponse sr = client().prepareSearch().setQuery(query).get();
        assertHitCount(sr, 2);
    }

    @Test
    public void testSearchRequestTemplateSource() throws Exception {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("_all");

        String query = "{ \"template\" : { \"query\": {\"match_{{template}}\": {} } }, \"params\" : { \"template\":\"all\" } }";
        BytesReference bytesRef = new BytesArray(query);
        searchRequest.templateSource(bytesRef, false);

        SearchResponse searchResponse = client().search(searchRequest).get();
        assertHitCount(searchResponse, 2);
    }

    @Test
    public void testThatParametersCanBeSet() throws Exception {
        index("test", "type", "1", jsonBuilder().startObject().field("theField", "foo").endObject());
        index("test", "type", "2", jsonBuilder().startObject().field("theField", "foo 2").endObject());
        index("test", "type", "3", jsonBuilder().startObject().field("theField", "foo 3").endObject());
        index("test", "type", "4", jsonBuilder().startObject().field("theField", "foo 4").endObject());
        index("test", "type", "5", jsonBuilder().startObject().field("otherField", "foo").endObject());
        refresh();

        Map<String, String> templateParams = Maps.newHashMap();
        templateParams.put("mySize", "2");
        templateParams.put("myField", "theField");
        templateParams.put("myValue", "foo");

        SearchResponse searchResponse = client().prepareSearch("test").setTypes("type").setTemplateName("full-query-template").setTemplateParams(templateParams).get();
        assertHitCount(searchResponse, 4);
        // size kicks in here...
        assertThat(searchResponse.getHits().getHits().length, is(2));

        templateParams.put("myField", "otherField");
        searchResponse = client().prepareSearch("test").setTypes("type").setTemplateName("full-query-template").setTemplateParams(templateParams).get();
        assertHitCount(searchResponse, 1);
    }
}
