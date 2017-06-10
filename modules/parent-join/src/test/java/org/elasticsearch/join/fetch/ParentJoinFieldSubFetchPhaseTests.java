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
package org.elasticsearch.join.fetch;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ParentJoinFieldSubFetchPhaseTests extends ESSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(ParentJoinPlugin.class);
    }

    public void testSingleParentJoinField() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
                .startObject("join_field")
                    .field("type", "join")
                    .field("parent", "child")
                    .field("child", "grand_child")
                    .field("product", "item")
                .endObject()
            .endObject()
            .endObject().string();
        IndexService service = createIndex("test", Settings.EMPTY);
        service.mapperService().merge("doc", new CompressedXContent(mapping),
            MapperService.MergeReason.MAPPING_UPDATE, true);

        // empty document
        client().prepareIndex("test", "doc", "0")
            .setSource().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // parent document
        client().prepareIndex("test", "doc", "1")
            .setSource("join_field", Collections.singletonMap("name", "parent"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // child document
        Map<String, String> joinField = new HashMap<>();
        joinField.put("name", "child");
        joinField.put("parent", "1");
        client().prepareIndex("test", "doc", "2")
            .setSource("join_field", joinField).setRouting("1")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // grand_child document
        joinField.clear();
        joinField.put("name", "grand_child");
        joinField.put("parent", "2");
        client().prepareIndex("test", "doc", "3")
            .setSource("join_field", joinField).setRouting("2")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // product document
        client().prepareIndex("test", "doc", "4")
            .setSource("join_field", Collections.singletonMap("name", "product"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // item document
        joinField.clear();
        joinField.put("name", "item");
        joinField.put("parent", "4");
        client().prepareIndex("test", "doc", "5")
            .setSource("join_field", joinField).setRouting("4").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        SearchResponse response = client().prepareSearch("test")
            .setQuery(QueryBuilders.termQuery("join_field", "parent"))
            .get();
        assertThat(response.getHits().totalHits, equalTo(1L));
        assertThat(response.getHits().getHits().length, equalTo(1));
        assertThat(response.getHits().getHits()[0].field("join_field").getValue(), equalTo("parent"));
        assertNull(response.getHits().getHits()[0].field("join_field#parent"));

        response = client().prepareSearch("test")
            .setQuery(QueryBuilders.termQuery("join_field", "child"))
            .get();
        assertThat(response.getHits().totalHits, equalTo(1L));
        assertThat(response.getHits().getHits().length, equalTo(1));
        assertThat(response.getHits().getHits()[0].field("join_field").getValue(), equalTo("child"));
        assertThat(response.getHits().getHits()[0].field("join_field#parent").getValue(), equalTo("1"));
        assertNull(response.getHits().getHits()[0].field("join_field#child"));

        response = client().prepareSearch("test")
            .setQuery(QueryBuilders.termQuery("join_field", "grand_child"))
            .get();
        assertThat(response.getHits().totalHits, equalTo(1L));
        assertThat(response.getHits().getHits().length, equalTo(1));
        assertThat(response.getHits().getHits()[0].field("join_field").getValue(), equalTo("grand_child"));
        assertThat(response.getHits().getHits()[0].field("join_field#child").getValue(), equalTo("2"));


        response = client().prepareSearch("test")
            .setQuery(QueryBuilders.termQuery("join_field", "product"))
            .get();
        assertThat(response.getHits().totalHits, equalTo(1L));
        assertThat(response.getHits().getHits().length, equalTo(1));
        assertThat(response.getHits().getHits()[0].field("join_field").getValue(), equalTo("product"));
        assertNull(response.getHits().getHits()[0].field("join_field#product"));

        response = client().prepareSearch("test")
            .setQuery(QueryBuilders.termQuery("join_field", "item"))
            .get();
        assertThat(response.getHits().totalHits, equalTo(1L));
        assertThat(response.getHits().getHits().length, equalTo(1));
        assertThat(response.getHits().getHits()[0].field("join_field").getValue(), equalTo("item"));
        assertThat(response.getHits().getHits()[0].field("join_field#product").getValue(), equalTo("4"));

        response = client().prepareSearch("test")
            .addSort(SortBuilders.fieldSort("join_field"))
            .get();
        assertThat(response.getHits().totalHits, equalTo(6L));
        assertThat(response.getHits().getHits().length, equalTo(6));
        assertThat(response.getHits().getHits()[0].field("join_field").getValue(), equalTo("child"));
        assertThat(response.getHits().getHits()[0].field("join_field#parent").getValue(), equalTo("1"));
        assertNull(response.getHits().getHits()[0].field("join_field#child"));
        assertThat(response.getHits().getHits()[1].field("join_field").getValue(), equalTo("grand_child"));
        assertThat(response.getHits().getHits()[1].field("join_field#child").getValue(), equalTo("2"));
        assertThat(response.getHits().getHits()[2].field("join_field").getValue(), equalTo("item"));
        assertThat(response.getHits().getHits()[2].field("join_field#product").getValue(), equalTo("4"));
        assertThat(response.getHits().getHits()[3].field("join_field").getValue(), equalTo("parent"));
        assertNull(response.getHits().getHits()[3].field("join_field#parent"));
        assertThat(response.getHits().getHits()[4].field("join_field").getValue(), equalTo("product"));
        assertNull(response.getHits().getHits()[4].field("join_field#product"));
        assertNull(response.getHits().getHits()[5].field("join_field"));
    }
}
