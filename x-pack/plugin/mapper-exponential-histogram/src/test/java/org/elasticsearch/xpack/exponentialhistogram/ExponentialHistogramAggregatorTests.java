/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.exponentialhistogram;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.TestDocumentParserContext;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.exponentialhistogram.agg.ExponentialHistogramAggregationBuilder;
import org.elasticsearch.xpack.exponentialhistogram.agg.InternalExponentialHistogram;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ExponentialHistogramAggregatorTests extends AggregatorTestCase {

    private static final String FIELD_NAME = "field";

    public void testHistograms() throws Exception {
        ExponentialHistogramFieldMapper mapper = new ExponentialHistogramFieldMapper.Builder(FIELD_NAME).build(
            MapperBuilderContext.root(false, false)
        );

        final int originalScale = 10;
        ExponentialHistogramFieldMapper.ExponentialHistogramBuckets negative =
            new ExponentialHistogramFieldMapper.ExponentialHistogramBuckets(0, List.of(1L, 0L, 0L, 2L));
        ExponentialHistogramFieldMapper.ExponentialHistogramBuckets positive =
            new ExponentialHistogramFieldMapper.ExponentialHistogramBuckets(0, List.of(1L, 0L, 0L, 2L));

        // Aggregate at a lower scale, illustrating that the aggregated histogram buckets are fewer and wider.
        ExponentialHistogramAggregationBuilder aggBuilder =
            new ExponentialHistogramAggregationBuilder("my_agg")
                .setMaxScale(9)
                .field(FIELD_NAME);

        testCase(iw -> {
            iw.addDocument(doc(mapper, originalScale, negative, positive));
            iw.addDocument(doc(mapper, originalScale, null, positive));
            iw.addDocument(doc(mapper, originalScale, negative, null));
        }, (InternalExponentialHistogram result) -> {

            List<InternalExponentialHistogram.Bucket> buckets = result.getBuckets();
            InternalExponentialHistogram.Bucket lastBucket = null;
            for (InternalExponentialHistogram.Bucket bucket : buckets) {
                assertThat("unordered bounds", bucket.getLowerBound(), lessThan(bucket.getUpperBound()));
                if (lastBucket != null) {
                    assertThat("unordered buckets", lastBucket.getUpperBound(), lessThanOrEqualTo(bucket.getLowerBound()));
                }
                lastBucket = bucket;
            }

            assertEquals(6, buckets.size());
            assertEquals(4, buckets.get(0).getCount());
            assertEquals(0, buckets.get(1).getCount());
            assertEquals(2, buckets.get(2).getCount());
            assertEquals(2, buckets.get(3).getCount());
            assertEquals(0, buckets.get(4).getCount());
            assertEquals(4, buckets.get(5).getCount());
        },  new AggTestConfig(aggBuilder, mapper.fieldType()));
    }

    public void testMaxBuckets() throws Exception {
        ExponentialHistogramFieldMapper mapper = new ExponentialHistogramFieldMapper.Builder(FIELD_NAME).build(
            MapperBuilderContext.root(false, false)
        );

        final int originalScale = 10;
        ExponentialHistogramFieldMapper.ExponentialHistogramBuckets positive =
            new ExponentialHistogramFieldMapper.ExponentialHistogramBuckets(0, List.of(1L, 2L, 3L));

        final int maxBuckets = 2;
        ExponentialHistogramAggregationBuilder aggBuilder =
            new ExponentialHistogramAggregationBuilder("my_agg")
                .setMaxScale(originalScale)
                .setMaxBuckets(maxBuckets)
                .field(FIELD_NAME);

        testCase(iw -> {
            iw.addDocument(doc(mapper, originalScale, null, positive));
        }, (InternalExponentialHistogram result) -> {
            List<InternalExponentialHistogram.Bucket> buckets = result.getBuckets();
            assertEquals(maxBuckets, buckets.size());
            assertEquals(9, result.getCurrentScale());
        },  new AggTestConfig(aggBuilder, mapper.fieldType()));
    }

    private List<IndexableField> doc(
        FieldMapper mapper,
        int scale,
        ExponentialHistogramFieldMapper.ExponentialHistogramBuckets negative,
        ExponentialHistogramFieldMapper.ExponentialHistogramBuckets positive
    ) {
        try {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON, out);

            builder.startObject();
            builder.field("scale", scale);
            if (negative != null) {
                builder.startObject("negative")
                    .field("offset", negative.offset)
                    .field("counts", negative.counts)
                    .endObject();
            }
            if (positive != null) {
                builder.startObject("positive")
                    .field("offset", positive.offset)
                    .field("counts", positive.counts)
                    .endObject();
            }
            builder.endObject();
            builder.close();

            final byte[] source = out.toByteArray();
            XContentParser parser = builder.contentType().xContent()
                .createParser(XContentParserConfiguration.EMPTY, new ByteArrayInputStream(source));
            parser.nextToken(); // move to first token
            TestDocumentParserContext ctx = new TestDocumentParserContext(
                MappingLookup.EMPTY,
                new SourceToParse("test", new BytesArray(source), XContentType.JSON)
            ) {
                @Override
                public XContentParser parser() {
                    return parser;
                }
            };
            mapper.parse(ctx);
            return ctx.doc().getFields();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new ExponentialHistogramMapperPlugin());
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new ExponentialHistogramAggregationBuilder("_name").field(fieldName);
    }
}
