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

import static java.util.Collections.singleton;

public class ExponentialHistogramAggregatorTests extends AggregatorTestCase {

    private static final String FIELD_NAME = "field";



    public void testHistograms() throws Exception {
        ExponentialHistogramFieldMapper mapper = new ExponentialHistogramFieldMapper.Builder(FIELD_NAME).build(
            MapperBuilderContext.root(false, false)
        );

        ExponentialHistogramFieldMapper.ExponentialHistogramBuckets negative =
            new ExponentialHistogramFieldMapper.ExponentialHistogramBuckets(0, List.of(1L, 0L, 0L, 2L));
        ExponentialHistogramFieldMapper.ExponentialHistogramBuckets positive =
            new ExponentialHistogramFieldMapper.ExponentialHistogramBuckets(0, List.of(1L, 0L, 0L, 2L));

        // TODO(axw) set scale
        ExponentialHistogramAggregationBuilder aggBuilder =
            new ExponentialHistogramAggregationBuilder("my_agg").field(FIELD_NAME);

        testCase(iw -> {
                iw.addDocument(doc(mapper, 10, negative, positive));
        }, (InternalExponentialHistogram result) -> {
            //result.getBuckets()
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

    /*
    private static BinaryDocValuesField exponentialHistogramFieldDocValues(String fieldName, double[] values) throws IOException {
        DoubleHistogram histogram = new DoubleHistogram(3);
        histogram.setAutoResize(true);
        for (double value : values) {
            histogram.recordValue(value);
        }
        BytesStreamOutput streamOutput = new BytesStreamOutput();
        for (DoubleHistogramIterationValue value : histogram.recordedValues()) {
            streamOutput.writeVInt((int) value.getCountAtValueIteratedTo());
            streamOutput.writeDouble(value.getValueIteratedTo());
        }
        return new BinaryDocValuesField(fieldName, streamOutput.bytes().toBytesRef());
    }
    */

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new ExponentialHistogramMapperPlugin());
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new ExponentialHistogramAggregationBuilder("_name").field(fieldName);
    }
}
