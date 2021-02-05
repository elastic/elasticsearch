/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class CopyToMapperIntegrationIT extends ESIntegTestCase {
    public void testDynamicTemplateCopyTo() throws Exception {
        assertAcked(
                client().admin().indices().prepareCreate("test-idx")
                        .setMapping(createDynamicTemplateMapping())
        );

        int recordCount = between(1, 200);

        for (int i = 0; i < recordCount * 2; i++) {
            client().prepareIndex("test-idx").setId(Integer.toString(i))
                    .setSource("test_field", "test " + i, "even", i % 2 == 0)
                    .get();
        }
        client().admin().indices().prepareRefresh("test-idx").execute().actionGet();

        SubAggCollectionMode aggCollectionMode = randomFrom(SubAggCollectionMode.values());

        SearchResponse response = client().prepareSearch("test-idx")
                .setQuery(QueryBuilders.termQuery("even", true))
                .addAggregation(AggregationBuilders.terms("test").field("test_field").size(recordCount * 2)
                        .collectMode(aggCollectionMode))
                .addAggregation(AggregationBuilders.terms("test_raw").field("test_field_raw").size(recordCount * 2)
                        .collectMode(aggCollectionMode))
                .execute().actionGet();

        assertThat(response.getHits().getTotalHits().value, equalTo((long) recordCount));

        assertThat(((Terms) response.getAggregations().get("test")).getBuckets().size(), equalTo(recordCount + 1));
        assertThat(((Terms) response.getAggregations().get("test_raw")).getBuckets().size(), equalTo(recordCount));

    }

    public void testDynamicObjectCopyTo() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("properties")
            .startObject("foo")
                .field("type", "text")
                .field("copy_to", "root.top.child")
            .endObject()
            .endObject().endObject());
        assertAcked(
            client().admin().indices().prepareCreate("test-idx")
                .setMapping(mapping)
        );
        client().prepareIndex("test-idx").setId("1")
            .setSource("foo", "bar")
            .get();
        client().admin().indices().prepareRefresh("test-idx").execute().actionGet();
        SearchResponse response = client().prepareSearch("test-idx")
            .setQuery(QueryBuilders.termQuery("root.top.child", "bar")).get();
        assertThat(response.getHits().getTotalHits().value, equalTo(1L));
    }

    private XContentBuilder createDynamicTemplateMapping() throws IOException {
        return XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startArray("dynamic_templates")

                .startObject().startObject("template_raw")
                .field("match", "*_raw")
                .field("match_mapping_type", "string")
                .startObject("mapping").field("type", "keyword").endObject()
                .endObject().endObject()

                .startObject().startObject("template_all")
                .field("match", "*")
                .field("match_mapping_type", "string")
                .startObject("mapping").field("type", "text").field("fielddata", true)
                .field("copy_to", "{name}_raw").endObject()
                .endObject().endObject()

                .endArray()
                .endObject().endObject();
    }

}
