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

package org.elasticsearch.index.mapper.analyzer;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.analysis.FieldNameAnalyzer;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class AnalyzerMapperIntegrationTests extends ElasticsearchIntegrationTest {

    @Test
    public void testAnalyzerMappingAppliedToDocs() throws Exception {

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_analyzer").field("path", "field_analyzer").endObject()
                .startObject("properties")
                .startObject("text").field("type", "string").endObject()
                .endObject()
                .endObject().endObject().string();
        prepareCreate("test").addMapping("type", mapping).get();
        XContentBuilder doc = XContentFactory.jsonBuilder().startObject().field("text", "foo bar").field("field_analyzer", "keyword");
        client().prepareIndex("test", "type").setSource(doc).get();
        client().admin().indices().prepareRefresh("test").get();
        SearchResponse response = client().prepareSearch("test").setQuery(QueryBuilders.termQuery("text", "foo bar")).get();
        assertThat(response.getHits().totalHits(), equalTo(1l));

        response = client().prepareSearch("test").setQuery(QueryBuilders.termQuery("field_analyzer", "keyword")).get();
        assertThat(response.getHits().totalHits(), equalTo(1l));
    }


}
