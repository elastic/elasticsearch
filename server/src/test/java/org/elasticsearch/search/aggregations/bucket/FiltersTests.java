/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator.KeyedFilter;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class FiltersTests extends BaseAggregationTestCase<FiltersAggregationBuilder> {

    @Override
    protected FiltersAggregationBuilder createTestAggregatorBuilder() {

        int size = randomIntBetween(1, 20);
        FiltersAggregationBuilder factory;
        if (randomBoolean()) {
            KeyedFilter[] filters = new KeyedFilter[size];
            int i = 0;
            for (String key : randomUnique(() -> randomAlphaOfLengthBetween(1, 20), size)) {
                filters[i++] = new KeyedFilter(
                    key,
                    QueryBuilders.termQuery(randomAlphaOfLengthBetween(5, 20), randomAlphaOfLengthBetween(5, 20))
                );
            }
            factory = new FiltersAggregationBuilder(randomAlphaOfLengthBetween(1, 20), filters);
        } else {
            QueryBuilder[] filters = new QueryBuilder[size];
            for (int i = 0; i < size; i++) {
                filters[i] = QueryBuilders.termQuery(randomAlphaOfLengthBetween(5, 20), randomAlphaOfLengthBetween(5, 20));
            }
            factory = new FiltersAggregationBuilder(randomAlphaOfLengthBetween(1, 20), filters);
        }
        if (randomBoolean()) {
            factory.otherBucket(randomBoolean());
        }
        if (randomBoolean()) {
            factory.otherBucketKey(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            factory.keyedBucket(randomBoolean());
        }
        return factory;
    }

    /**
     * Test that when passing in keyed filters as list or array, the list stored internally is sorted by key
     * Also check the list passed in is not modified by this but rather copied
     */
    public void testFiltersSortedByKey() {
        KeyedFilter[] original = new KeyedFilter[] {
            new KeyedFilter("bbb", new MatchNoneQueryBuilder()),
            new KeyedFilter("aaa", new MatchNoneQueryBuilder()) };
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
        builder.startArray("filters").startObject().startObject("term").field("field", "foo").endObject().endObject().endArray();
        builder.endObject();
        try (XContentParser parser = createParser(shuffleXContent(builder))) {
            parser.nextToken();
            FiltersAggregationBuilder filters = FiltersAggregationBuilder.parse("agg_name", parser);
            // The other bucket is disabled by default
            assertFalse(filters.otherBucket());

            builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            builder.startObject();
            builder.startArray("filters").startObject().startObject("term").field("field", "foo").endObject().endObject().endArray();
            builder.field("other_bucket_key", "some_key");
            builder.endObject();
        }
        try (XContentParser parser = createParser(shuffleXContent(builder))) {
            parser.nextToken();
            FiltersAggregationBuilder filters = FiltersAggregationBuilder.parse("agg_name", parser);
            // but setting a key enables it automatically
            assertTrue(filters.otherBucket());

            builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            builder.startObject();
            builder.startArray("filters").startObject().startObject("term").field("field", "foo").endObject().endObject().endArray();
            builder.field("other_bucket", false);
            builder.field("other_bucket_key", "some_key");
            builder.endObject();
        }
        try (XContentParser parser = createParser(shuffleXContent(builder))) {
            parser.nextToken();
            FiltersAggregationBuilder filters = FiltersAggregationBuilder.parse("agg_name", parser);
            // unless the other bucket is explicitly disabled
            assertFalse(filters.otherBucket());
        }
    }

    public void testRewrite() throws IOException {
        // test non-keyed filter that doesn't rewrite
        AggregationBuilder original = new FiltersAggregationBuilder("my-agg", new MatchAllQueryBuilder());
        original.setMetadata(Collections.singletonMap(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20)));
        AggregationBuilder rewritten = original.rewrite(new QueryRewriteContext(parserConfig(), null, () -> 0L));
        assertSame(original, rewritten);

        // test non-keyed filter that does rewrite
        original = new FiltersAggregationBuilder("my-agg", new BoolQueryBuilder());
        rewritten = original.rewrite(new QueryRewriteContext(parserConfig(), null, () -> 0L));
        assertNotSame(original, rewritten);
        assertThat(rewritten, instanceOf(FiltersAggregationBuilder.class));
        assertEquals("my-agg", ((FiltersAggregationBuilder) rewritten).getName());
        assertEquals(1, ((FiltersAggregationBuilder) rewritten).filters().size());
        assertEquals("0", ((FiltersAggregationBuilder) rewritten).filters().get(0).key());
        assertThat(((FiltersAggregationBuilder) rewritten).filters().get(0).filter(), instanceOf(MatchAllQueryBuilder.class));
        assertFalse(((FiltersAggregationBuilder) rewritten).isKeyed());

        // test keyed filter that doesn't rewrite
        original = new FiltersAggregationBuilder("my-agg", new KeyedFilter("my-filter", new MatchAllQueryBuilder()));
        rewritten = original.rewrite(new QueryRewriteContext(parserConfig(), null, () -> 0L));
        assertSame(original, rewritten);

        // test non-keyed filter that does rewrite
        original = new FiltersAggregationBuilder("my-agg", new KeyedFilter("my-filter", new BoolQueryBuilder()));
        rewritten = original.rewrite(new QueryRewriteContext(parserConfig(), null, () -> 0L));
        assertNotSame(original, rewritten);
        assertThat(rewritten, instanceOf(FiltersAggregationBuilder.class));
        assertEquals("my-agg", ((FiltersAggregationBuilder) rewritten).getName());
        assertEquals(1, ((FiltersAggregationBuilder) rewritten).filters().size());
        assertEquals("my-filter", ((FiltersAggregationBuilder) rewritten).filters().get(0).key());
        assertThat(((FiltersAggregationBuilder) rewritten).filters().get(0).filter(), instanceOf(MatchAllQueryBuilder.class));
        assertTrue(((FiltersAggregationBuilder) rewritten).isKeyed());

        // test sub-agg filter that does rewrite
        original = new TermsAggregationBuilder("terms").userValueTypeHint(ValueType.BOOLEAN)
            .subAggregation(new FiltersAggregationBuilder("my-agg", new KeyedFilter("my-filter", new BoolQueryBuilder())));
        rewritten = original.rewrite(new QueryRewriteContext(parserConfig(), null, () -> 0L));
        assertNotSame(original, rewritten);
        assertNotEquals(original, rewritten);
        assertThat(rewritten, instanceOf(TermsAggregationBuilder.class));
        assertThat(rewritten.getSubAggregations().size(), equalTo(1));
        AggregationBuilder subAgg = rewritten.getSubAggregations().iterator().next();
        assertThat(subAgg, instanceOf(FiltersAggregationBuilder.class));
        assertNotSame(original.getSubAggregations().iterator().next(), subAgg);
        assertEquals("my-agg", subAgg.getName());
        assertSame(rewritten, rewritten.rewrite(new QueryRewriteContext(parserConfig(), null, () -> 0L)));
    }

    public void testRewritePreservesOtherBucket() throws IOException {
        FiltersAggregationBuilder originalFilters = new FiltersAggregationBuilder("my-agg", new BoolQueryBuilder());
        originalFilters.otherBucket(randomBoolean());
        originalFilters.otherBucketKey(randomAlphaOfLength(10));

        AggregationBuilder rewritten = originalFilters.rewrite(new QueryRewriteContext(parserConfig(), null, () -> 0L));
        assertThat(rewritten, instanceOf(FiltersAggregationBuilder.class));

        FiltersAggregationBuilder rewrittenFilters = (FiltersAggregationBuilder) rewritten;
        assertEquals(originalFilters.otherBucket(), rewrittenFilters.otherBucket());
        assertEquals(originalFilters.otherBucketKey(), rewrittenFilters.otherBucketKey());
    }

    public void testEmptyFilters() throws IOException {
        {
            XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            builder.startObject();
            builder.startArray("filters").endArray();  // unkeyed array
            builder.endObject();
            XContentParser parser = createParser(shuffleXContent(builder));
            parser.nextToken();
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> FiltersAggregationBuilder.parse("agg_name", parser)
            );
            assertThat(e.getMessage(), equalTo("[filters] cannot be empty."));
        }

        {
            XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            builder.startObject();
            builder.startObject("filters").endObject(); // keyed object
            builder.endObject();
            XContentParser parser = createParser(shuffleXContent(builder));
            parser.nextToken();
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> FiltersAggregationBuilder.parse("agg_name", parser)
            );
            assertThat(e.getMessage(), equalTo("[filters] cannot be empty."));
        }
    }
}
