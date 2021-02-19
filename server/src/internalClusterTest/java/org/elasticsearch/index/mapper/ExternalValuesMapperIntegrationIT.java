/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.locationtech.jts.geom.Coordinate;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;

public class ExternalValuesMapperIntegrationIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ExternalMapperPlugin.class);
    }

    public void testHighlightingOnCustomString() throws Exception {
        prepareCreate("test-idx").setMapping(
            XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties")
                .startObject("field").field("type", FakeStringFieldMapper.CONTENT_TYPE).endObject()
                .endObject()
                .endObject().endObject()).execute().get();

        index("test-idx", "1", XContentFactory.jsonBuilder()
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
        assertThat(response.getHits().getTotalHits().value, equalTo(1L));
        assertThat(response.getHits().getAt(0).getHighlightFields().size(), equalTo(0));

        // make sure it is not excluded when we explicitly provide the fieldname
        response = client().prepareSearch("test-idx")
            .setQuery(QueryBuilders.matchQuery("field", "exactly the same"))
            .highlighter(new HighlightBuilder().field("field"))
            .execute().actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value, equalTo(1L));
        assertThat(response.getHits().getAt(0).getHighlightFields().size(), equalTo(1));
        assertThat(response.getHits().getAt(0).getHighlightFields().get("field").fragments()[0].string(), equalTo("Every day is " +
            "<em>exactly</em> <em>the</em> <em>same</em>"));

        // make sure it is not excluded when we explicitly provide the fieldname and a wildcard
        response = client().prepareSearch("test-idx")
            .setQuery(QueryBuilders.matchQuery("field", "exactly the same"))
            .highlighter(new HighlightBuilder().field("*").field("field"))
            .execute().actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value, equalTo(1L));
        assertThat(response.getHits().getAt(0).getHighlightFields().size(), equalTo(1));
        assertThat(response.getHits().getAt(0).getHighlightFields().get("field").fragments()[0].string(), equalTo("Every day is " +
            "<em>exactly</em> <em>the</em> <em>same</em>"));
    }

    public void testExternalValues() throws Exception {
        prepareCreate("test-idx").setMapping(
                XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties")
                    .startObject("field").field("type", ExternalMapperPlugin.EXTERNAL).endObject()
                .endObject()
            .endObject().endObject()).execute().get();

        index("test-idx", "1", XContentFactory.jsonBuilder()
                .startObject()
                    .field("field", "1234")
                .endObject());
        refresh();

        SearchResponse response;

        response = client().prepareSearch("test-idx")
                .setPostFilter(QueryBuilders.termQuery("field.bool", "true"))
                .execute().actionGet();

        assertThat(response.getHits().getTotalHits().value, equalTo((long) 1));

        response = client().prepareSearch("test-idx")
                .setPostFilter(QueryBuilders.geoDistanceQuery("field.point").point(42.0, 51.0).distance("1km"))
                .execute().actionGet();

        assertThat(response.getHits().getTotalHits().value, equalTo((long) 1));

        response = client().prepareSearch("test-idx")
                .setPostFilter(QueryBuilders.geoShapeQuery("field.shape",
                    new EnvelopeBuilder(new Coordinate(-101, 46), new Coordinate(-99, 44))).relation(ShapeRelation.WITHIN))
                        .execute().actionGet();

        assertThat(response.getHits().getTotalHits().value, equalTo((long) 1));

        response = client().prepareSearch("test-idx")
                .setPostFilter(QueryBuilders.termQuery("field.field", "foo"))
                .execute().actionGet();

        assertThat(response.getHits().getTotalHits().value, equalTo((long) 1));
    }

    public void testExternalValuesWithMultifield() throws Exception {
        prepareCreate("test-idx").setMapping(
                XContentFactory.jsonBuilder().startObject().startObject("_doc").startObject("properties")
                .startObject("f")
                    .field("type", ExternalMapperPlugin.EXTERNAL_UPPER)
                    .startObject("fields")
                        .startObject("g")
                            .field("type", "keyword")
                            .field("store", true)
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject().endObject()).execute().get();

        indexDoc("test-idx", "1", "f", "This is my text");
        refresh();

        SearchResponse response = client().prepareSearch("test-idx")
                .setQuery(QueryBuilders.termQuery("f.g", "FOO BAR"))
                .execute().actionGet();

        assertThat(response.getHits().getTotalHits().value, equalTo((long) 1));
    }
}
