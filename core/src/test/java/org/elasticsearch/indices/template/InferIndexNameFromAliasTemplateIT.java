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

import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESIntegTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class InferIndexNameFromAliasTemplateIT extends ESIntegTestCase {

    @After
    public void finalizeTest() {
        cleanupTemplates();
    }

    @Before
    public void initTest() {
        cleanupTemplates();
    }

    public void testInferFalseForTemplate() throws Exception {

        client().admin().indices().preparePutTemplate("template_1")
                .setPatterns(Collections.singletonList("ab*cd*"))
                .setSettings(indexSettings())
                .setOrder(0)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field1").field("type", "text").field("store", true).endObject()
                        .startObject("field2").field("type", "keyword").field("store", true).endObject()
                        .endObject().endObject().endObject())
                .setAliases(
                        "    {\n" +
                                "        \"{index}-alias1\" : {},\n" +
                                "        \"{index}-alias2\" : {\n" +
                                "            \"filter\" : {\n" +
                                "                \"type\" : {\n" +
                                "                    \"value\" : \"type2\"\n" +
                                "                }\n" +
                                "            }\n" +
                                "         },\n" +
                                "        \"{index}-alias3\" : { \"routing\" : \"1\" }" +
                                "    }\n")
                .setInferIndexNameFromAlias(false)
                .get();

        GetIndexTemplatesResponse getTemplatesResponse = client().admin().indices().prepareGetTemplates().get();

        assertThat(getTemplatesResponse.getIndexTemplates().get(0).isInferIndexNameFromAlias(), equalTo(false));
    }

    public void testInferTrueForTemplate() throws Exception {
        client().admin().indices().preparePutTemplate("template_1")
                .setPatterns(Collections.singletonList("ab*cd*"))
                .setSettings(indexSettings())
                .setOrder(0)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field1").field("type", "text").field("store", true).endObject()
                        .startObject("field2").field("type", "keyword").field("store", true).endObject()
                        .endObject().endObject().endObject())
                .setAliases(
                        "    {\n" +
                                "        \"{index}-alias1\" : {},\n" +
                                "        \"{index}-alias2\" : {\n" +
                                "            \"filter\" : {\n" +
                                "                \"type\" : {\n" +
                                "                    \"value\" : \"type2\"\n" +
                                "                }\n" +
                                "            }\n" +
                                "         },\n" +
                                "        \"{index}-alias3\" : { \"routing\" : \"1\" }" +
                                "    }\n")
                .setInferIndexNameFromAlias(true)
                .get();

        GetIndexTemplatesResponse getTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertThat(getTemplatesResponse.getIndexTemplates().get(0).isInferIndexNameFromAlias(), equalTo(true));
    }

    public void testInferMissingForTemplate() throws Exception {
        client().admin().indices().preparePutTemplate("template_1")
                .setPatterns(Collections.singletonList("ab*cd*"))
                .setSettings(indexSettings())
                .setOrder(0)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field1").field("type", "text").field("store", true).endObject()
                        .startObject("field2").field("type", "keyword").field("store", true).endObject()
                        .endObject().endObject().endObject())
                .setAliases(
                        "    {\n" +
                                "        \"{index}-alias1\" : {},\n" +
                                "        \"{index}-alias2\" : {\n" +
                                "            \"filter\" : {\n" +
                                "                \"type\" : {\n" +
                                "                    \"value\" : \"type2\"\n" +
                                "                }\n" +
                                "            }\n" +
                                "         },\n" +
                                "        \"{index}-alias3\" : { \"routing\" : \"1\" }" +
                                "    }\n")
                .get();

        GetIndexTemplatesResponse getTemplatesResponse = client().admin().indices().prepareGetTemplates().get();

        assertThat(getTemplatesResponse.getIndexTemplates().get(0).isInferIndexNameFromAlias(), equalTo(false));
    }

    public void testSimpleInferWithOrder() throws Exception {

        client().admin().indices().preparePutTemplate("basic_template_1")
                .setPatterns(Collections.singletonList("ab*cd*"))
                .setSettings(indexSettings())
                .setOrder(0)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field1").field("type", "text").field("store", true).endObject()
                        .startObject("field2").field("type", "keyword").field("store", true).endObject()
                        .endObject().endObject().endObject())
                .setAliases(
                        "    {\n" +
                                "        \"{index}1-alias1\" : {},\n" +
                                "        \"{index}1-alias2\" : {\n" +
                                "            \"filter\" : {\n" +
                                "                \"type\" : {\n" +
                                "                    \"value\" : \"type2\"\n" +
                                "                }\n" +
                                "            }\n" +
                                "         },\n" +
                                "        \"{index}1-alias3\" : { \"routing\" : \"1\" }" +
                                "    }\n")
                .setInferIndexNameFromAlias(true)
                .get();

        client().admin().indices().preparePutTemplate("basic_template_2")
                .setPatterns(Collections.singletonList("ab*cde*"))
                .setSettings(indexSettings())
                .setOrder(1)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field2").field("type", "text").field("store", false).endObject()
                        .endObject().endObject().endObject())
                .setAliases(
                        "    {\n" +
                                "        \"{index}-alias1\" : {},\n" +
                                "        \"{index}-alias2\" : {\n" +
                                "            \"filter\" : {\n" +
                                "                \"type\" : {\n" +
                                "                    \"value\" : \"type2\"\n" +
                                "                }\n" +
                                "            }\n" +
                                "         },\n" +
                                "        \"{index}-alias3\" : { \"routing\" : \"1\" }" +
                                "    }\n")
                .setInferIndexNameFromAlias(true)
                .get();

        GetIndexTemplatesResponse getTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertThat(getTemplatesResponse.getIndexTemplates(), hasSize(2));

        // index something into ab123cde-alias1, will match on both templates
        client().prepareIndex("ab123cde-alias1", "type1", "1")
                .setSource("field1", "value1", "field2", "value 2")
                .setRefreshPolicy(IMMEDIATE).get();

        ensureGreen();
        SearchResponse searchResponse = client().prepareSearch("ab123cde")
                .setQuery(termQuery("field1", "value1"))
                .addStoredField("field1").addStoredField("field2")
                .execute().actionGet();

        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getAt(0).field("field1").value().toString(), equalTo("value1"));
        // field2 is not stored.
        assertThat(searchResponse.getHits().getAt(0).field("field2"), nullValue());

        // index something into ab123cde-alias1, will match only template (order 0)
        client().prepareIndex("ab123cd01-alias1", "type1", "1")
                .setSource("field1", "value1", "field2", "value 2")
                .setRefreshPolicy(IMMEDIATE).get();

        ensureGreen();
        // now only match on one template (template_1)
        searchResponse = client().prepareSearch("ab123cd0")
                .setQuery(termQuery("field1", "value1"))
                .addStoredField("field1").addStoredField("field2")
                .execute().actionGet();
        if (searchResponse.getFailedShards() > 0) {
            logger.warn("failed search {}", Arrays.toString(searchResponse.getShardFailures()));
        }
        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getAt(0).field("field1").value().toString(), equalTo("value1"));
        assertThat(searchResponse.getHits().getAt(0).field("field2").value().toString(), equalTo("value 2"));

        // verify templates metadata
        for (IndexTemplateMetaData indexTemplateMetaData : getTemplatesResponse.getIndexTemplates()) {
            assertThat(indexTemplateMetaData.isInferIndexNameFromAlias(), equalTo(true));
        }

        // verify indices metadata
        GetIndexResponse allIndices = client().admin().indices().prepareGetIndex().get();
        // first index matched templates template_1 & template_2, hence got 6 aliases
        // index named was inferred from alias "ab123cde-alias1" to "ab123cde"
        assertThat(allIndices.getAliases().containsKey("ab123cde"), equalTo(true));
        assertThat(allIndices.getAliases().get("ab123cde").size(), equalTo(6));
        Set<String> aliasesNames = allIndices.getAliases().get("ab123cde").stream().map(AliasMetaData::getAlias)
                .collect(Collectors.toSet());
        assertThat(aliasesNames.size(), equalTo(6));
        assertThat(aliasesNames.contains("ab123cde-alias1"), equalTo(true));
        assertThat(aliasesNames.contains("ab123cde-alias2"), equalTo(true));
        assertThat(aliasesNames.contains("ab123cde-alias3"), equalTo(true));
        assertThat(aliasesNames.contains("ab123cde1-alias1"), equalTo(true));
        assertThat(aliasesNames.contains("ab123cde1-alias2"), equalTo(true));
        assertThat(aliasesNames.contains("ab123cde1-alias3"), equalTo(true));

        assertThat(allIndices.getSettings().containsKey("ab123cde"), equalTo(true));
        assertThat(allIndices.getSettings().get("ab123cde").get(IndexMetaData.SETTING_INDEX_PROVIDED_NAME), equalTo("ab123cde"));

        // second index matched templates template_1, hence got 3 aliases
        // index named was inferred from alias "ab123cd01-alias1" to "ab123cd0"
        assertThat(allIndices.getAliases().containsKey("ab123cd0"), equalTo(true));
        assertThat(allIndices.getAliases().get("ab123cd0").size(), equalTo(3));
        aliasesNames = allIndices.getAliases().get("ab123cd0").stream().map(AliasMetaData::getAlias).collect(Collectors.toSet());
        assertThat(aliasesNames.size(), equalTo(3));
        assertThat(aliasesNames.contains("ab123cd01-alias1"), equalTo(true));
        assertThat(aliasesNames.contains("ab123cd01-alias2"), equalTo(true));
        assertThat(aliasesNames.contains("ab123cd01-alias3"), equalTo(true));

        assertThat(allIndices.getSettings().containsKey("ab123cd0"), equalTo(true));
        assertThat(allIndices.getSettings().get("ab123cd0").get(IndexMetaData.SETTING_INDEX_PROVIDED_NAME), equalTo("ab123cd0"));
    }

    public void testComplexInfer() throws Exception {
        client().admin().indices().preparePutTemplate("complex_infer")
                .setPatterns(Collections.singletonList("ab*cd*"))
                .setSettings(indexSettings())
                .setOrder(0)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field1").field("type", "text").field("store", true).endObject()
                        .startObject("field2").field("type", "keyword").field("store", false).endObject()
                        .endObject().endObject().endObject())
                .setAliases(
                        "    {\n" +
                                "        \"prefix-{index}-inner-{index}-suffix\" : {},\n" +
                                "        \"humble-{index}\" : {}\n" +
                                "    }\n")
                .setInferIndexNameFromAlias(true)
                .get();

        GetIndexTemplatesResponse getTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertThat(getTemplatesResponse.getIndexTemplates(), hasSize(1));

        // index something into ab123cde-alias1, will match on both templates
        client().prepareIndex("prefix-ab123cd321-inner-ab123cd321-suffix", "type1", "1")
                .setSource("field1", "value1", "field2", "value 2")
                .setRefreshPolicy(IMMEDIATE).get();

        ensureGreen();
        SearchResponse searchResponse = client().prepareSearch("prefix-ab123cd321-inner-ab123cd321-suffix")
                .setQuery(termQuery("field1", "value1"))
                .addStoredField("field1").addStoredField("field2")
                .execute().actionGet();

        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getAt(0).field("field1").value().toString(), equalTo("value1"));
        // field2 is not stored.
        assertThat(searchResponse.getHits().getAt(0).field("field2"), nullValue());

        // verify templates metadata
        for (IndexTemplateMetaData indexTemplateMetaData : getTemplatesResponse.getIndexTemplates()) {
            assertThat(indexTemplateMetaData.isInferIndexNameFromAlias(), equalTo(true));
        }

        // verify indices metadata
        GetIndexResponse allIndices = client().admin().indices().prepareGetIndex().get();
        // index named was inferred from alias "prefix-ab123cd321-inner-ab123cd321-suffix" to "ab123cd321"
        assertThat(allIndices.getAliases().containsKey("ab123cd321"), equalTo(true));
        assertThat(allIndices.getAliases().get("ab123cd321").size(), equalTo(2));
        Set<String> aliasesNames = allIndices.getAliases().get("ab123cd321").stream().map(AliasMetaData::getAlias)
                .collect(Collectors.toSet());
        assertThat(aliasesNames.size(), equalTo(2));
        assertThat(aliasesNames.contains("prefix-ab123cd321-inner-ab123cd321-suffix"), equalTo(true));
        assertThat(aliasesNames.contains("humble-ab123cd321"), equalTo(true));

        assertThat(allIndices.getSettings().containsKey("ab123cd321"), equalTo(true));
        assertThat(allIndices.getSettings().get("ab123cd321").get(IndexMetaData.SETTING_INDEX_PROVIDED_NAME), equalTo("ab123cd321"));
    }

    public void testInferTrueWithNoAliases() throws Exception {
        client().admin().indices().preparePutTemplate("no_aliases")
                .setPatterns(Collections.singletonList("ab*cd*"))
                .setSettings(indexSettings())
                .setOrder(0)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field1").field("type", "text").field("store", true).endObject()
                        .startObject("field2").field("type", "keyword").field("store", false).endObject()
                        .endObject().endObject().endObject())
                .setInferIndexNameFromAlias(true)
                .get();

        GetIndexTemplatesResponse getTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertThat(getTemplatesResponse.getIndexTemplates(), hasSize(1));

        client().prepareIndex("ab123cd-xxx", "type1", "1")
                .setSource("field1", "value1", "field2", "value 2")
                .setRefreshPolicy(IMMEDIATE).get();

        ensureGreen();
        SearchResponse searchResponse = client().prepareSearch("ab123cd-xxx")
                .setQuery(termQuery("field1", "value1"))
                .addStoredField("field1").addStoredField("field2")
                .execute().actionGet();

        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getAt(0).field("field1").value().toString(), equalTo("value1"));
        // field2 is not stored.
        assertThat(searchResponse.getHits().getAt(0).field("field2"), nullValue());

        // verify templates metadata
        for (IndexTemplateMetaData indexTemplateMetaData : getTemplatesResponse.getIndexTemplates()) {
            assertThat(indexTemplateMetaData.isInferIndexNameFromAlias(), equalTo(true));
        }

        // verify indices metadata
        GetIndexResponse allIndices = client().admin().indices().prepareGetIndex().get();
        // no index name is as given, no aliases were created
        assertThat(allIndices.getAliases().size(), equalTo(0));

        assertThat(allIndices.getSettings().containsKey("ab123cd-xxx"), equalTo(true));
        assertThat(allIndices.getSettings().get("ab123cd-xxx").get(IndexMetaData.SETTING_INDEX_PROVIDED_NAME), equalTo("ab123cd-xxx"));
    }

    public void testInferTrueWithNoMatchInAliasName() throws Exception {
        client().admin().indices().preparePutTemplate("no_infer_match")
                .setPatterns(Collections.singletonList("ab*cd*"))
                .setSettings(indexSettings())
                .setOrder(0)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field1").field("type", "text").field("store", true).endObject()
                        .startObject("field2").field("type", "keyword").field("store", false).endObject()
                        .endObject().endObject().endObject())
                .setAliases(
                        "    {\n" +
                                "        \"{index}-inner-{index}-suffix\" : {},\n" +
                                "        \"humble-{index}\" : {}\n" +
                                "    }\n")
                .setInferIndexNameFromAlias(true)
                .get();

        GetIndexTemplatesResponse getTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertThat(getTemplatesResponse.getIndexTemplates(), hasSize(1));

        client().prepareIndex("ab123cd321-inner-ab123cd32z-suffix", "type1", "1")
                .setSource("field1", "value1", "field2", "value 2")
                .setRefreshPolicy(IMMEDIATE).get();

        ensureGreen();
        SearchResponse searchResponse = client().prepareSearch("ab123cd321-inner-ab123cd32z-suffix")
                .setQuery(termQuery("field1", "value1"))
                .addStoredField("field1").addStoredField("field2")
                .execute().actionGet();

        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getAt(0).field("field1").value().toString(), equalTo("value1"));
        // field2 is not stored.
        assertThat(searchResponse.getHits().getAt(0).field("field2"), nullValue());

        // verify templates metadata
        for (IndexTemplateMetaData indexTemplateMetaData : getTemplatesResponse.getIndexTemplates()) {
            assertThat(indexTemplateMetaData.isInferIndexNameFromAlias(), equalTo(true));
        }

        // verify indices metadata
        GetIndexResponse allIndices = client().admin().indices().prepareGetIndex().get();
        // index named was inferred from alias "prefix-ab123cd321-inner-ab123cd321-suffix" to "ab123cd321"
        assertThat(allIndices.getAliases().containsKey("ab123cd321-inner-ab123cd32z-suffix"), equalTo(true));
        assertThat(allIndices.getAliases().get("ab123cd321-inner-ab123cd32z-suffix").size(), equalTo(2));
        Set<String> aliasesNames = allIndices.getAliases().get("ab123cd321-inner-ab123cd32z-suffix").stream().map(AliasMetaData::getAlias)
                .collect(Collectors.toSet());
        assertThat(aliasesNames.size(), equalTo(2));
        assertThat(aliasesNames.contains("ab123cd321-inner-ab123cd32z-suffix-inner-ab123cd321-inner-ab123cd32z-suffix-suffix"),
                equalTo(true));
        assertThat(aliasesNames.contains("humble-ab123cd321-inner-ab123cd32z-suffix"), equalTo(true));

        assertThat(allIndices.getSettings().containsKey("ab123cd321-inner-ab123cd32z-suffix"), equalTo(true));
        assertThat(allIndices.getSettings().get("ab123cd321-inner-ab123cd32z-suffix").get(IndexMetaData.SETTING_INDEX_PROVIDED_NAME),
                equalTo("ab123cd321-inner-ab123cd32z-suffix"));
    }

    public void testInferTrueWithDateMath() throws Exception {

        DateTime now = new DateTime(DateTimeZone.UTC);
        String today = DateTimeFormat.forPattern("YYYY.MM.dd").print(now);
        String yesterday = DateTimeFormat.forPattern("YYYY.MM.dd").print(now.minusDays(1));
        String theDayBeforeYesterday = DateTimeFormat.forPattern("YYYY.MM.dd").print(now.minusDays(2));


        client().admin().indices().preparePutTemplate("date_math_basic")
                .setPatterns(Collections.singletonList("ab*cd*ef*"))
                .setSettings(indexSettings())
                .setOrder(0)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field1").field("type", "text").field("store", true).endObject()
                        .startObject("field2").field("type", "keyword").field("store", false).endObject()
                        .endObject().endObject().endObject())
                .setAliases(
                        "    {\n" +
                                "        \"{index}-suffix\" : {},\n" +
                                "        \"prefix-{index}\" : {}\n" +
                                "    }\n")
                .setInferIndexNameFromAlias(true)
                .get();

        GetIndexTemplatesResponse getTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertThat(getTemplatesResponse.getIndexTemplates(), hasSize(1));

        // index something into ab123cde-alias1, will match on both templates
        client().prepareIndex("<ab-{now/d}-cd-{now/d-1d}-ef-{now/d-2d}-suffix>", "type1", "1")
                .setSource("field1", "value1", "field2", "value 2")
                .setRefreshPolicy(IMMEDIATE).get();

        ensureGreen();
        SearchResponse searchResponse = client().prepareSearch("ab*")
                .setQuery(termQuery("field1", "value1"))
                .addStoredField("field1").addStoredField("field2")
                .execute().actionGet();

        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getAt(0).field("field1").value().toString(), equalTo("value1"));
        // field2 is not stored.
        assertThat(searchResponse.getHits().getAt(0).field("field2"), nullValue());

        // verify templates metadata
        for (IndexTemplateMetaData indexTemplateMetaData : getTemplatesResponse.getIndexTemplates()) {
            assertThat(indexTemplateMetaData.isInferIndexNameFromAlias(), equalTo(true));
        }

        // verify indices metadata
        GetIndexResponse allIndices = client().admin().indices().prepareGetIndex().get();
        // index named was inferred from alias "prefix-ab123cd321-inner-ab123cd321-suffix" to "ab123cd321"
        final String expectedIndexName = "ab-" + today + "-cd-" + yesterday + "-ef-" + theDayBeforeYesterday;

        assertThat("Expected: " + expectedIndexName + ", got (ignore[]): " + allIndices.getAliases(),
                allIndices.getAliases().containsKey(expectedIndexName), equalTo(true));
        assertThat(allIndices.getAliases().get(expectedIndexName).size(), equalTo(2));
        Set<String> aliasesNames = allIndices.getAliases().get(expectedIndexName).stream().map(AliasMetaData::getAlias)
                .collect(Collectors.toSet());
        assertThat(aliasesNames.size(), equalTo(2));
        assertThat(aliasesNames.contains("prefix-" + expectedIndexName), equalTo(true));
        assertThat(aliasesNames.contains(expectedIndexName + "-suffix"), equalTo(true));

        assertThat(allIndices.getSettings().containsKey(expectedIndexName), equalTo(true));
        assertThat(allIndices.getSettings().get(expectedIndexName).get(IndexMetaData.SETTING_INDEX_PROVIDED_NAME),
                equalTo("<ab-{now/d}-cd-{now/d-1d}-ef-{now/d-2d}>"));
    }

    public void testIndexTemplateWithAliasesInSource() {
        client().admin().indices().preparePutTemplate("template_1")
                .setSource("{\n" +
                        "    \"template\" : \"*\",\n" +
                        "    \"aliases\" : {\n" +
                        "        \"{index}-blah\" : {\n" +
                        "            \"filter\" : {\n" +
                        "                \"type\" : {\n" +
                        "                    \"value\" : \"type2\"\n" +
                        "                }\n" +
                        "            }\n" +
                        "        }\n" +
                        "    },\n" +
                        "    \"infer_index_name_from_alias\" : true\n" +
                        "}")
                .get();


        assertAcked(prepareCreate("test_index-blah").addMapping("type1").addMapping("type2"));
        ensureGreen();

        GetAliasesResponse getAliasesResponse = client().admin().indices().prepareGetAliases().setIndices("test_index").get();
        assertThat(getAliasesResponse.getAliases().size(), equalTo(1));
        assertThat(getAliasesResponse.getAliases().containsKey("test_index"), equalTo(true));
        assertThat(getAliasesResponse.getAliases().get("test_index").size(), equalTo(1));

        client().prepareIndex("test_index", "type1", "1").setSource("field", "value1").get();
        client().prepareIndex("test_index", "type2", "2").setSource("field", "value2").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test_index").get();
        assertHitCount(searchResponse, 2L);

        searchResponse = client().prepareSearch("test_index-blah").get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getAt(0).type(), equalTo("type2"));

        // verify templates metadata
        GetIndexTemplatesResponse getTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        for (IndexTemplateMetaData indexTemplateMetaData : getTemplatesResponse.getIndexTemplates()) {
            assertThat(indexTemplateMetaData.isInferIndexNameFromAlias(), equalTo(true));
        }

        // verify indices metadata
        GetIndexResponse allIndices = client().admin().indices().prepareGetIndex().get();
        // index named was inferred from alias "test_index-blah" to "test_index"
        assertThat(allIndices.getAliases().containsKey("test_index"), equalTo(true));
        assertThat(allIndices.getAliases().get("test_index").size(), equalTo(1));
        Set<String> aliasesNames = allIndices.getAliases().get("test_index").stream().map(AliasMetaData::getAlias)
                .collect(Collectors.toSet());
        assertThat(aliasesNames.size(), equalTo(1));
        assertThat(aliasesNames.contains("test_index-blah"), equalTo(true));

        assertThat(allIndices.getSettings().containsKey("test_index"), equalTo(true));
        assertThat(allIndices.getSettings().get("test_index").get(IndexMetaData.SETTING_INDEX_PROVIDED_NAME), equalTo("test_index"));
    }

    private static void cleanupTemplates() {
        client().admin().indices().prepareDeleteTemplate("*").get();

        // check get all templates on an empty index.
        GetIndexTemplatesResponse getTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertThat(getTemplatesResponse.getIndexTemplates(), empty());
    }
}
