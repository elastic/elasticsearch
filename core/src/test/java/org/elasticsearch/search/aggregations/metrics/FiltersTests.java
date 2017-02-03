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

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregator.KeyedFilter;

import java.io.IOException;

public class FiltersTests extends BaseAggregationTestCase<FiltersAggregationBuilder> {

    @Override
    protected FiltersAggregationBuilder createTestAggregatorBuilder() {

        int size = randomIntBetween(1, 20);
        FiltersAggregationBuilder factory;
        if (randomBoolean()) {
            KeyedFilter[] filters = new KeyedFilter[size];
            int i = 0;
            for (String key : randomUnique(() -> randomAsciiOfLengthBetween(1, 20), size)) {
                filters[i++] = new KeyedFilter(key,
                        QueryBuilders.termQuery(randomAsciiOfLengthBetween(5, 20), randomAsciiOfLengthBetween(5, 20)));
            }
            factory = new FiltersAggregationBuilder(randomAsciiOfLengthBetween(1, 20), filters);
        } else {
            QueryBuilder[] filters = new QueryBuilder[size];
            for (int i = 0; i < size; i++) {
                filters[i] = QueryBuilders.termQuery(randomAsciiOfLengthBetween(5, 20), randomAsciiOfLengthBetween(5, 20));
            }
            factory = new FiltersAggregationBuilder(randomAsciiOfLengthBetween(1, 20), filters);
        }
        if (randomBoolean()) {
            factory.otherBucket(randomBoolean());
        }
        if (randomBoolean()) {
            factory.otherBucketKey(randomAsciiOfLengthBetween(1, 20));
        }
        return factory;
    }

    /**
     * Test that when passing in keyed filters as list or array, the list stored internally is sorted by key
     * Also check the list passed in is not modified by this but rather copied
     */
    public void testFiltersSortedByKey() {
        KeyedFilter[] original = new KeyedFilter[]{new KeyedFilter("bbb", new MatchNoneQueryBuilder()),
                new KeyedFilter("aaa", new MatchNoneQueryBuilder())};
        FiltersAggregationBuilder builder;
        builder = new FiltersAggregationBuilder("my-agg", original);
        assertEquals("aaa", builder.filters().get(0).key());
        assertEquals("bbb", builder.filters().get(1).key());
        // original should be unchanged
        assertEquals("bbb", original[0].key());
        assertEquals("aaa", original[1].key());
    }

    public void testOtherBucket() throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        builder.startObject();
        builder.startArray("filters").endArray();
        builder.endObject();
        XContentParser parser = createParser(shuffleXContent(builder));
        parser.nextToken();
        QueryParseContext context = new QueryParseContext(parser);
        FiltersAggregationBuilder filters = FiltersAggregationBuilder.parse("agg_name", context);
        // The other bucket is disabled by default
        assertFalse(filters.otherBucket());

        builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        builder.startObject();
        builder.startArray("filters").endArray();
        builder.field("other_bucket_key", "some_key");
        builder.endObject();
        parser = createParser(shuffleXContent(builder));
        parser.nextToken();
        context = new QueryParseContext(parser);
        filters = FiltersAggregationBuilder.parse("agg_name", context);
        // but setting a key enables it automatically
        assertTrue(filters.otherBucket());

        builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        builder.startObject();
        builder.startArray("filters").endArray();
        builder.field("other_bucket", false);
        builder.field("other_bucket_key", "some_key");
        builder.endObject();
        parser = createParser(shuffleXContent(builder));
        parser.nextToken();
        context = new QueryParseContext(parser);
        filters = FiltersAggregationBuilder.parse("agg_name", context);
        // unless the other bucket is explicitly disabled
        assertFalse(filters.otherBucket());
    }
}
