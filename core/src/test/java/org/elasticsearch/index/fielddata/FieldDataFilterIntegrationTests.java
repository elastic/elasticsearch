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

package org.elasticsearch.index.fielddata;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class FieldDataFilterIntegrationTests extends ElasticsearchIntegrationTest {

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    @Test
    public void testRegexpFilter() throws IOException {
        CreateIndexRequestBuilder builder = prepareCreate("test");
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("name")
                        .field("type", "string")
                        .startObject("fielddata")
                            .startObject("filter")
                                .startObject("regex")
                                    .field("pattern", "^bac.*")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                    .startObject("not_filtered")
                        .field("type", "string")
                    .endObject()
                .endObject()
                .endObject().endObject();
        assertAcked(builder.addMapping("type", mapping));
        ensureGreen();
        int numDocs = scaledRandomIntBetween(5, 50);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test", "type", "" + 0).setSource("name", "bacon bastards", "not_filtered", "bacon bastards").get();
        }
        refresh();
        SearchResponse searchResponse = client().prepareSearch()
                .setSize(0)
                .setQuery(matchAllQuery())
                .addAggregation(terms("name").field("name"))
                .addAggregation(terms("not_filtered").field("not_filtered")).get();
        Aggregations aggs = searchResponse.getAggregations();
        Terms nameAgg = aggs.get("name");
        assertThat(nameAgg.getBuckets().size(), Matchers.equalTo(1));
        assertThat(nameAgg.getBuckets().iterator().next().getKeyAsString(), Matchers.equalTo("bacon"));
        
        Terms notFilteredAgg = aggs.get("not_filtered");
        assertThat(notFilteredAgg.getBuckets().size(), Matchers.equalTo(2));
        assertThat(notFilteredAgg.getBuckets().get(0).getKeyAsString(), Matchers.isOneOf("bacon", "bastards"));
        assertThat(notFilteredAgg.getBuckets().get(1).getKeyAsString(), Matchers.isOneOf("bacon", "bastards"));
    }

}
