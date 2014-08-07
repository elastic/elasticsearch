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

package org.elasticsearch.index.query.guice;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class IndexQueryParserModuleTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testCustomInjection() {
        Settings settings = settingsBuilder()
                .put("index.queryparser.query.my.type", MyJsonQueryParser.class)
                .put("index.queryparser.query.my.param1", "value1")
                .put("index.queryparser.filter.my.type", MyJsonFilterParser.class)
                .put("index.queryparser.filter.my.param2", "value2")
                .put("index.cache.filter.type", "none")
                .put("name", "IndexQueryParserModuleTests")
                .build();

        IndexQueryParserService indexQueryParserService = createIndex("test", settings).queryParserService();

        MyJsonQueryParser myJsonQueryParser = (MyJsonQueryParser) indexQueryParserService.queryParser("my");

        assertThat(myJsonQueryParser.names()[0], equalTo("my"));
        assertThat(myJsonQueryParser.settings().get("param1"), equalTo("value1"));

        MyJsonFilterParser myJsonFilterParser = (MyJsonFilterParser) indexQueryParserService.filterParser("my");
        assertThat(myJsonFilterParser.names()[0], equalTo("my"));
        assertThat(myJsonFilterParser.settings().get("param2"), equalTo("value2"));
    }
}
