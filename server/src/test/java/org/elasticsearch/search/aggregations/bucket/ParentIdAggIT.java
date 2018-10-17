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
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class ParentIdAggIT extends ESIntegTestCase {
    @Override
    protected boolean forbidPrivateIndexSettings() {
        // this is needed to force the index version since _parent field is disabled on this version
        return false;
    }

    public void testParentIdAggregation() throws IOException {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_6_0_0, Version.V_6_4_0);
        XContentBuilder mapping = jsonBuilder().startObject()
                .startObject("childtype")
                    .startObject("_parent")
                        .field("type", "parenttype")
                    .endObject()
                .endObject()
                .endObject();
        assertAcked(prepareCreate("testidx")
            .addMapping("childtype", mapping)
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build()));
        client().prepareIndex("testidx", "childtype")
            .setSource(jsonBuilder().startObject().field("num", 1).endObject())
            .setParent("p1")
            .get();
        client().prepareIndex("testidx", "childtype")
            .setSource(jsonBuilder().startObject().field("num", 2).endObject())
            .setParent("p1")
            .get();

        refresh();
        ensureGreen("testidx");
        SearchResponse searchResponse = client().prepareSearch("testidx")
            .setTypes("childtype")
            .setQuery(matchAllQuery())
            .addAggregation(AggregationBuilders.terms("children").field("_parent#parenttype")).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2L));
        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getAggregations().getAsMap().get("children"), instanceOf(Terms.class));
        Terms terms = (Terms) searchResponse.getAggregations().getAsMap().get("children");
        assertThat(terms.getBuckets().iterator().next().getDocCount(), equalTo(2L));
    }
}
