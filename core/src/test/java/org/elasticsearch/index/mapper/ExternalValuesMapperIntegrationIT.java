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

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilders;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;

public class ExternalValuesMapperIntegrationIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ExternalMapperPlugin.class);
    }

    public void testHighlightingOnCustomString() throws Exception {
        prepareCreate("test-idx").addMapping("type",
            XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field").field("type", FakeStringFieldMapper.CONTENT_TYPE).endObject()
                .endObject()
                .endObject().endObject()).execute().get();

        index("test-idx", "type", "1", XContentFactory.jsonBuilder()
            .startObject()
            .field("field", "Every day is exactly the same")
            .endObject());
        refresh();

        SearchResponse response;
        // test if the highlighting is excluded when we use wildcards
        response = client().prepareSearch("test-idx")
            .setQuery(QueryBuilders.matchQuery("field", "exactly the same"))
            .highlighter(new HighlightBuilder().field("*"))
            .execute().actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits(), equalTo(1L));
        assertThat(response.getHits().getAt(0).getHighlightFields().size(), equalTo(0));

        // make sure it is not excluded when we explicitly provide the fieldname
        response = client().prepareSearch("test-idx")
            .setQuery(QueryBuilders.matchQuery("field", "exactly the same"))
            .highlighter(new HighlightBuilder().field("field"))
            .execute().actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits(), equalTo(1L));
        assertThat(response.getHits().getAt(0).getHighlightFields().size(), equalTo(1));
        assertThat(response.getHits().getAt(0).getHighlightFields().get("field").fragments()[0].string(), equalTo("Every day is " +
            "<em>exactly</em> <em>the</em> <em>same</em>"));

        // make sure it is not excluded when we explicitly provide the fieldname and a wildcard
        response = client().prepareSearch("test-idx")
            .setQuery(QueryBuilders.matchQuery("field", "exactly the same"))
            .highlighter(new HighlightBuilder().field("*").field("field"))
            .execute().actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits(), equalTo(1L));
        assertThat(response.getHits().getAt(0).getHighlightFields().size(), equalTo(1));
        assertThat(response.getHits().getAt(0).getHighlightFields().get("field").fragments()[0].string(), equalTo("Every day is " +
            "<em>exactly</em> <em>the</em> <em>same</em>"));
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

        index("test-idx", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                    .field("field", "1234")
                .endObject());
        refresh();

        SearchResponse response;

        response = client().prepareSearch("test-idx")
                .setPostFilter(QueryBuilders.termQuery("field.bool", "true"))
                .execute().actionGet();

        assertThat(response.getHits().getTotalHits(), equalTo((long) 1));

        response = client().prepareSearch("test-idx")
                .setPostFilter(QueryBuilders.geoDistanceQuery("field.point").point(42.0, 51.0).distance("1km"))
                .execute().actionGet();

        assertThat(response.getHits().getTotalHits(), equalTo((long) 1));

        response = client().prepareSearch("test-idx")
                .setPostFilter(QueryBuilders.geoShapeQuery("field.shape", ShapeBuilders.newPoint(-100, 45)).relation(ShapeRelation.WITHIN))
                        .execute().actionGet();

        assertThat(response.getHits().getTotalHits(), equalTo((long) 1));

        response = client().prepareSearch("test-idx")
                .setPostFilter(QueryBuilders.termQuery("field.field", "foo"))
                .execute().actionGet();

        assertThat(response.getHits().getTotalHits(), equalTo((long) 1));
    }

    public void testExternalValuesWithMultifield() throws Exception {
        prepareCreate("test-idx").addMapping("doc",
                XContentFactory.jsonBuilder().startObject().startObject("doc").startObject("properties")
                .startObject("f")
                    .field("type", ExternalMapperPlugin.EXTERNAL_UPPER)
                    .startObject("fields")
                        .startObject("g")
                            .field("type", "text")
                            .field("store", true)
                            .startObject("fields")
                                .startObject("raw")
                                    .field("type", "keyword")
                                    .field("store", true)
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject().endObject()).execute().get();

        index("test-idx", "doc", "1", "f", "This is my text");
        refresh();

        SearchResponse response = client().prepareSearch("test-idx")
                .setQuery(QueryBuilders.termQuery("f.g.raw", "FOO BAR"))
                .execute().actionGet();

        assertThat(response.getHits().getTotalHits(), equalTo((long) 1));
    }
}
