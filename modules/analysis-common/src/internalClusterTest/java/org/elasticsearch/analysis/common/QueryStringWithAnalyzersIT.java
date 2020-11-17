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

package org.elasticsearch.analysis.common;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class QueryStringWithAnalyzersIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CommonAnalysisPlugin.class);
    }

    /**
     * Validates that we properly split fields using the word delimiter filter in query_string.
     */
    public void testCustomWordDelimiterQueryString() {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder()
                        .put("analysis.analyzer.my_analyzer.type", "custom")
                        .put("analysis.analyzer.my_analyzer.tokenizer", "whitespace")
                        .put("analysis.analyzer.my_analyzer.filter", "custom_word_delimiter")
                        .put("analysis.filter.custom_word_delimiter.type", "word_delimiter")
                        .put("analysis.filter.custom_word_delimiter.generate_word_parts", "true")
                        .put("analysis.filter.custom_word_delimiter.generate_number_parts", "false")
                        .put("analysis.filter.custom_word_delimiter.catenate_numbers", "true")
                        .put("analysis.filter.custom_word_delimiter.catenate_words", "false")
                        .put("analysis.filter.custom_word_delimiter.split_on_case_change", "false")
                        .put("analysis.filter.custom_word_delimiter.split_on_numerics", "false")
                        .put("analysis.filter.custom_word_delimiter.stem_english_possessive", "false"))
                .setMapping(
                        "field1", "type=text,analyzer=my_analyzer",
                        "field2", "type=text,analyzer=my_analyzer"));

        client().prepareIndex("test").setId("1").setSource(
                "field1", "foo bar baz",
                "field2", "not needed").get();
        refresh();

        SearchResponse response = client()
                .prepareSearch("test")
                .setQuery(
                        queryStringQuery("foo.baz").defaultOperator(Operator.AND)
                                .field("field1").field("field2")).get();
        assertHitCount(response, 1L);
    }
}
