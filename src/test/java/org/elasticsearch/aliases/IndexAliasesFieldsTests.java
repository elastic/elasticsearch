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

package org.elasticsearch.aliases;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 */
public class IndexAliasesFieldsTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testQuery() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=string", "field2", "type=string")
                .addAlias(new Alias("alias1").fields("field2"))
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefresh(true)
                .get();

        SearchResponse response = client().prepareSearch("test").setQuery(matchQuery("field1", "value1")).get();
        assertHitCount(response, 1);
        response = client().prepareSearch("alias1").setQuery(matchQuery("field1", "value1")).get();
        assertHitCount(response, 0);

        client().admin().indices().prepareAliases().addAlias(new String[]{"test"}, "alias2", null, "field1").get();

        response = client().prepareSearch("test").setQuery(matchQuery("field2", "value2")).get();
        assertHitCount(response, 1);
        response = client().prepareSearch("alias2").setQuery(matchQuery("field2", "value2")).get();
        assertHitCount(response, 0);
    }

    @Test
    public void testFields() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=string,store=yes", "field2", "type=string,store=yes")
                        .addAlias(new Alias("alias1").fields("field2"))
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefresh(true)
                .get();

        SearchResponse response = client().prepareSearch("test").addField("field1").get();
        assertThat(response.getHits().getAt(0).fields().get("field1").<String>getValue(), equalTo("value1"));
        response = client().prepareSearch("alias1").addField("field1").get();
        assertThat(response.getHits().getAt(0).fields().get("field1"), nullValue());

        client().admin().indices().prepareAliases().addAlias(new String[] {"test"}, "alias2", null, "field1").get();

        response = client().prepareSearch("test").addField("field2").get();
        assertThat(response.getHits().getAt(0).fields().get("field2").<String>getValue(), equalTo("value2"));
        response = client().prepareSearch("alias2").addField("field2").get();
        assertThat(response.getHits().getAt(0).fields().get("field2"), nullValue());
    }

}
