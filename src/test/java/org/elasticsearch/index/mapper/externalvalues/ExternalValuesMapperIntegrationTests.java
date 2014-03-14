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
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class ExternalValuesMapperIntegrationTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put("plugin.types", ExternalMapperPlugin.class.getName())
                .put(super.nodeSettings(nodeOrdinal))
                .build();
    }

    @Test
    public void testExternalGeoPoint() throws Exception {
        prepareCreate("test-idx").addMapping("doc", createMapping()).execute().get();
        ensureYellow("test-idx");

        index("test-idx", "doc", "1", "external", "dummy");
        refresh();

        SearchResponse response;

        response = client().prepareSearch("test-idx")
                .setPostFilter(FilterBuilders.termFilter("external.bool", "T"))
                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo((long) 1));

        response = client().prepareSearch("test-idx")
                .setPostFilter(FilterBuilders.geoDistanceRangeFilter("external.point").point(42.0, 51.0).to("1km"))
                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo((long) 1));

        response = client().prepareSearch("test-idx")
                .setPostFilter(FilterBuilders.geoShapeFilter("external.shape", ShapeBuilder.newPoint(-100, 45), ShapeRelation.WITHIN))
                        .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo((long) 1));
    }


    private XContentBuilder createMapping() throws IOException {
        return XContentFactory.jsonBuilder().startObject().startObject("doc").startObject("properties")
                .startObject("external").field("type", RegisterExternalTypes.EXTERNAL).endObject()
                .endObject().endObject().endObject();
    }

}
