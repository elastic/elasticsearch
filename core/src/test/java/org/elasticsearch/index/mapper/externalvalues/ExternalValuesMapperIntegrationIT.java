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

package org.elasticsearch.index.mapper.externalvalues;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilders;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;

import static org.hamcrest.Matchers.equalTo;

public class ExternalValuesMapperIntegrationIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(ExternalMapperPlugin.class);
    }

    public void testExternalValues() throws Exception {
        prepareCreate("test-idx").addMapping("type",
                XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject(ExternalMetadataMapper.CONTENT_TYPE)
                .endObject()
                .startObject("properties")
                    .startObject("field").field("type", ExternalMapperPlugin.EXTERNAL).endObject()
                .endObject()
            .endObject().endObject()).execute().get();
        ensureYellow("test-idx");

        index("test-idx", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                    .field("field", "1234")
                .endObject());
        refresh();

        SearchResponse response;

        response = client().prepareSearch("test-idx")
                .setPostFilter(QueryBuilders.termQuery("field.bool", "T"))
                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo((long) 1));

        response = client().prepareSearch("test-idx")
                .setPostFilter(QueryBuilders.geoDistanceRangeQuery("field.point", 42.0, 51.0).to("1km"))
                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo((long) 1));

        response = client().prepareSearch("test-idx")
                .setPostFilter(QueryBuilders.geoShapeQuery("field.shape", ShapeBuilders.newPoint(-100, 45)).relation(ShapeRelation.WITHIN))
                        .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo((long) 1));

        response = client().prepareSearch("test-idx")
                .setPostFilter(QueryBuilders.termQuery("field.field", "foo"))
                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo((long) 1));
    }

    public void testExternalValuesWithMultifield() throws Exception {
        prepareCreate("test-idx").addMapping("doc",
                XContentFactory.jsonBuilder().startObject().startObject("doc").startObject("properties")
                .startObject("f")
                    .field("type", ExternalMapperPlugin.EXTERNAL_UPPER)
                    .startObject("fields")
                        .startObject("f")
                            .field("type", "string")
                            .field("store", "yes")
                            .startObject("fields")
                                .startObject("raw")
                                    .field("type", "string")
                                    .field("index", "not_analyzed")
                                    .field("store", "yes")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject().endObject()).execute().get();
        ensureYellow("test-idx");

        index("test-idx", "doc", "1", "f", "This is my text");
        refresh();

        SearchResponse response = client().prepareSearch("test-idx")
                .setQuery(QueryBuilders.termQuery("f.f.raw", "FOO BAR"))
                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo((long) 1));
    }
}
