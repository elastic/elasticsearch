/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.WrapperQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilters;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

public class FiltersAggsRewriteIT extends ESSingleNodeTestCase {

    public void testWrapperQueryIsRewritten() throws IOException {
        createIndex("test", Settings.EMPTY, "test", "title", "type=text");
        prepareIndex("test").setId("1").setSource("title", "foo bar baz").get();
        prepareIndex("test").setId("2").setSource("title", "foo foo foo").get();
        prepareIndex("test").setId("3").setSource("title", "bar baz bax").get();
        client().admin().indices().prepareRefresh("test").get();

        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference bytesReference;
        try (XContentBuilder builder = XContentFactory.contentBuilder(xContentType)) {
            builder.startObject();
            {
                builder.startObject("terms");
                {
                    builder.array("title", "foo");
                }
                builder.endObject();
            }
            builder.endObject();
            bytesReference = BytesReference.bytes(builder);
        }
        FiltersAggregationBuilder builder = new FiltersAggregationBuilder(
            "titles",
            new FiltersAggregator.KeyedFilter("titleterms", new WrapperQueryBuilder(bytesReference))
        );
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        builder.setMetadata(metadata);
        assertResponse(client().prepareSearch("test").setSize(0).addAggregation(builder), response -> {
            assertEquals(3, response.getHits().getTotalHits().value());
            InternalFilters filters = response.getAggregations().get("titles");
            assertEquals(1, filters.getBuckets().size());
            assertEquals(2, filters.getBuckets().get(0).getDocCount());
            assertEquals(metadata, filters.getMetadata());
        });
    }
}
