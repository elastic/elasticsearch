/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.aggregations.bucket.timeseries;

import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.aggregations.bucket.AggregationTestCase;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.RoutingPathFields;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TimeSeriesAggregatorTests extends AggregationTestCase {

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of();
    }

    public void testStandAloneTimeSeriesWithSum() throws IOException {
        TimeSeriesAggregationBuilder aggregationBuilder = new TimeSeriesAggregationBuilder("ts").subAggregation(sum("sum").field("val1"));
        long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2021-01-01T00:00:00Z");
        timeSeriesTestCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            writeTS(iw, startTime + 1, new Object[] { "dim1", "aaa", "dim2", "xxx" }, new Object[] { "val1", 1 });
            writeTS(iw, startTime + 2, new Object[] { "dim1", "aaa", "dim2", "yyy" }, new Object[] { "val1", 2 });
            writeTS(iw, startTime + 3, new Object[] { "dim1", "bbb", "dim2", "zzz" }, new Object[] { "val1", 3 });
            writeTS(iw, startTime + 4, new Object[] { "dim1", "bbb", "dim2", "zzz" }, new Object[] { "val1", 4 });
            writeTS(iw, startTime + 5, new Object[] { "dim1", "aaa", "dim2", "xxx" }, new Object[] { "val1", 5 });
            writeTS(iw, startTime + 6, new Object[] { "dim1", "aaa", "dim2", "yyy" }, new Object[] { "val1", 6 });
            writeTS(iw, startTime + 7, new Object[] { "dim1", "bbb", "dim2", "zzz" }, new Object[] { "val1", 7 });
            writeTS(iw, startTime + 8, new Object[] { "dim1", "bbb", "dim2", "zzz" }, new Object[] { "val1", 8 });
        }, ts -> {
            assertThat(ts.getBuckets(), hasSize(3));

            assertThat(ts.getBucketByKey("{dim1=aaa, dim2=xxx}").docCount, equalTo(2L));
            assertThat(((Sum) ts.getBucketByKey("{dim1=aaa, dim2=xxx}").getAggregations().get("sum")).value(), equalTo(6.0));
            assertThat(ts.getBucketByKey("{dim1=aaa, dim2=yyy}").docCount, equalTo(2L));
            assertThat(((Sum) ts.getBucketByKey("{dim1=aaa, dim2=yyy}").getAggregations().get("sum")).value(), equalTo(8.0));
            assertThat(ts.getBucketByKey("{dim1=bbb, dim2=zzz}").docCount, equalTo(4L));
            assertThat(((Sum) ts.getBucketByKey("{dim1=bbb, dim2=zzz}").getAggregations().get("sum")).value(), equalTo(22.0));

        },
            new KeywordFieldMapper.Builder("dim1", IndexVersion.current()).dimension(true)
                .build(MapperBuilderContext.root(true, true))
                .fieldType(),
            new KeywordFieldMapper.Builder("dim2", IndexVersion.current()).dimension(true)
                .build(MapperBuilderContext.root(true, true))
                .fieldType(),
            new NumberFieldMapper.NumberFieldType("val1", NumberFieldMapper.NumberType.INTEGER)
        );
    }

    public static void writeTS(RandomIndexWriter iw, long timestamp, Object[] dimensions, Object[] metrics) throws IOException {
        final List<IndexableField> fields = new ArrayList<>();
        fields.add(new SortedNumericDocValuesField(DataStreamTimestampFieldMapper.DEFAULT_PATH, timestamp));
        fields.add(new LongPoint(DataStreamTimestampFieldMapper.DEFAULT_PATH, timestamp));
        RoutingPathFields routingPathFields = new RoutingPathFields(null);
        for (int i = 0; i < dimensions.length; i += 2) {
            if (dimensions[i + 1] instanceof Number n) {
                routingPathFields.addLong(dimensions[i].toString(), n.longValue());
                if (dimensions[i + 1] instanceof Integer || dimensions[i + 1] instanceof Long) {
                    fields.add(new NumericDocValuesField(dimensions[i].toString(), ((Number) dimensions[i + 1]).longValue()));
                } else if (dimensions[i + 1] instanceof Float) {
                    fields.add(new FloatDocValuesField(dimensions[i].toString(), (float) dimensions[i + 1]));
                } else if (dimensions[i + 1] instanceof Double) {
                    fields.add(new DoubleDocValuesField(dimensions[i].toString(), (double) dimensions[i + 1]));
                }
            } else {
                routingPathFields.addString(dimensions[i].toString(), dimensions[i + 1].toString());
                fields.add(new SortedSetDocValuesField(dimensions[i].toString(), new BytesRef(dimensions[i + 1].toString())));
            }
        }
        for (int i = 0; i < metrics.length; i += 2) {
            if (metrics[i + 1] instanceof Integer || metrics[i + 1] instanceof Long) {
                fields.add(new NumericDocValuesField(metrics[i].toString(), ((Number) metrics[i + 1]).longValue()));
            } else if (metrics[i + 1] instanceof Float) {
                fields.add(new FloatDocValuesField(metrics[i].toString(), (float) metrics[i + 1]));
            } else if (metrics[i + 1] instanceof Double) {
                fields.add(new DoubleDocValuesField(metrics[i].toString(), (double) metrics[i + 1]));
            }
        }
        fields.add(
            new SortedDocValuesField(TimeSeriesIdFieldMapper.NAME, TimeSeriesIdFieldMapper.buildLegacyTsid(routingPathFields).toBytesRef())
        );
        iw.addDocument(fields);
    }

    public void testWithDateHistogramExecutedAsFilterByFilterWithTimeSeriesIndexSearcher() throws IOException {
        DateHistogramAggregationBuilder aggregationBuilder = new DateHistogramAggregationBuilder("by_timestamp").field("@timestamp")
            .fixedInterval(DateHistogramInterval.HOUR)
            .subAggregation(new TimeSeriesAggregationBuilder("ts").subAggregation(sum("sum").field("val1")));

        // Before this threw a CollectionTerminatedException because FilterByFilterAggregation#getLeafCollector() always returns a
        // LeafBucketCollector.NO_OP_COLLECTOR instance. And TimeSeriesIndexSearcher can't deal with this when initializing the
        // leaf walkers.
        testCase(iw -> {
            long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-01-01T00:00:00Z");
            for (int i = 1; i <= 5000; i++) {
                writeTS(iw, startTime++, new Object[] { "dim1", "aaa" }, new Object[] { "val1", 1 });
            }
        }, internalAggregation -> {
            InternalDateHistogram dateHistogram = (InternalDateHistogram) internalAggregation;
            assertThat(dateHistogram.getBuckets(), hasSize(1));
            InternalTimeSeries timeSeries = dateHistogram.getBuckets().get(0).getAggregations().get("ts");
            assertThat(timeSeries.getBuckets(), hasSize(1));
            Sum sum = timeSeries.getBuckets().get(0).getAggregations().get("sum");
            assertThat(sum.value(), equalTo(5000.0));
        },
            new AggTestConfig(
                aggregationBuilder,
                TimeSeriesIdFieldMapper.FIELD_TYPE_BYTEREF,
                new DateFieldMapper.DateFieldType("@timestamp"),
                new KeywordFieldMapper.Builder("dim1", IndexVersion.current()).dimension(true)
                    .build(MapperBuilderContext.root(true, true))
                    .fieldType(),
                new NumberFieldMapper.NumberFieldType("val1", NumberFieldMapper.NumberType.INTEGER)
            ).withQuery(new MatchAllDocsQuery())
        );
    }

    public void testMultiBucketAggregationAsSubAggregation() throws IOException {
        long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-01-01T00:00:00Z");
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            writeTS(iw, startTime + 1, new Object[] { "dim1", "aaa", "dim2", "xxx" }, new Object[] {});
            writeTS(iw, startTime + 2, new Object[] { "dim1", "aaa", "dim2", "yyy" }, new Object[] {});
            writeTS(iw, startTime + 3, new Object[] { "dim1", "bbb", "dim2", "zzz" }, new Object[] {});
            writeTS(iw, startTime + 4, new Object[] { "dim1", "bbb", "dim2", "zzz" }, new Object[] {});
            writeTS(iw, startTime + 5, new Object[] { "dim1", "aaa", "dim2", "xxx" }, new Object[] {});
            writeTS(iw, startTime + 6, new Object[] { "dim1", "aaa", "dim2", "yyy" }, new Object[] {});
            writeTS(iw, startTime + 7, new Object[] { "dim1", "bbb", "dim2", "zzz" }, new Object[] {});
            writeTS(iw, startTime + 8, new Object[] { "dim1", "bbb", "dim2", "zzz" }, new Object[] {});
        };
        Consumer<InternalTimeSeries> verifier = ts -> {
            assertThat(ts.getBuckets(), hasSize(3));

            assertThat(ts.getBucketByKey("{dim1=aaa, dim2=xxx}").docCount, equalTo(2L));
            InternalDateHistogram byTimeStampBucket = ts.getBucketByKey("{dim1=aaa, dim2=xxx}").getAggregations().get("by_timestamp");
            assertThat(
                byTimeStampBucket.getBuckets(),
                contains(new InternalDateHistogram.Bucket(startTime, 2, null, InternalAggregations.EMPTY))
            );
            assertThat(ts.getBucketByKey("{dim1=aaa, dim2=yyy}").docCount, equalTo(2L));
            byTimeStampBucket = ts.getBucketByKey("{dim1=aaa, dim2=yyy}").getAggregations().get("by_timestamp");
            assertThat(
                byTimeStampBucket.getBuckets(),
                contains(new InternalDateHistogram.Bucket(startTime, 2, null, InternalAggregations.EMPTY))
            );
            assertThat(ts.getBucketByKey("{dim1=bbb, dim2=zzz}").docCount, equalTo(4L));
            byTimeStampBucket = ts.getBucketByKey("{dim1=bbb, dim2=zzz}").getAggregations().get("by_timestamp");
            assertThat(
                byTimeStampBucket.getBuckets(),
                contains(new InternalDateHistogram.Bucket(startTime, 4, null, InternalAggregations.EMPTY))
            );
        };

        DateHistogramAggregationBuilder dateBuilder = new DateHistogramAggregationBuilder("by_timestamp");
        dateBuilder.field("@timestamp");
        dateBuilder.fixedInterval(DateHistogramInterval.seconds(1));
        TimeSeriesAggregationBuilder tsBuilder = new TimeSeriesAggregationBuilder("by_tsid");
        tsBuilder.subAggregation(dateBuilder);
        timeSeriesTestCase(
            tsBuilder,
            new MatchAllDocsQuery(),
            buildIndex,
            verifier,
            new KeywordFieldMapper.Builder("dim1", IndexVersion.current()).dimension(true)
                .build(MapperBuilderContext.root(true, true))
                .fieldType(),
            new KeywordFieldMapper.Builder("dim2", IndexVersion.current()).dimension(true)
                .build(MapperBuilderContext.root(true, true))
                .fieldType()
        );
    }

    public void testAggregationSize() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = multiTsWriter();

        List<Consumer<InternalTimeSeries>> verifiers = new ArrayList<>();

        verifiers.add(ts -> assertThat(ts.getBucketByKey("{dim1=aaa, dim2=xxx}").docCount, equalTo(2L)));
        verifiers.add(ts -> assertThat(ts.getBucketByKey("{dim1=aaa, dim2=yyy}").docCount, equalTo(2L)));
        verifiers.add(ts -> assertThat(ts.getBucketByKey("{dim1=bbb, dim2=zzz}").docCount, equalTo(2L)));

        for (int i = 1; i < 3; i++) {
            int size = i;
            Consumer<InternalTimeSeries> limitedVerifier = ts -> {
                assertThat(ts.getBuckets(), hasSize(size));

                for (int j = 0; j < size; j++) {
                    verifiers.get(j).accept(ts);
                }
            };

            TimeSeriesAggregationBuilder limitedTsBuilder = new TimeSeriesAggregationBuilder("by_tsid");
            limitedTsBuilder.setSize(i);
            timeSeriesTestCase(
                limitedTsBuilder,
                new MatchAllDocsQuery(),
                buildIndex,
                limitedVerifier,
                new KeywordFieldMapper.Builder("dim1", IndexVersion.current()).dimension(true)
                    .build(MapperBuilderContext.root(true, true))
                    .fieldType(),
                new KeywordFieldMapper.Builder("dim2", IndexVersion.current()).dimension(true)
                    .build(MapperBuilderContext.root(true, true))
                    .fieldType()
            );
        }
    }

    private CheckedConsumer<RandomIndexWriter, IOException> multiTsWriter() {
        long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-01-01T00:00:00Z");
        return iw -> {
            writeTS(iw, startTime + 1, new Object[] { "dim1", "aaa", "dim2", "xxx" }, new Object[] { "val1", 1 });
            writeTS(iw, startTime + 2, new Object[] { "dim1", "aaa", "dim2", "yyy" }, new Object[] { "val1", 2 });
            writeTS(iw, startTime + 3, new Object[] { "dim1", "bbb", "dim2", "zzz" }, new Object[] { "val1", 3 });
            writeTS(iw, startTime + 4, new Object[] { "dim1", "bbb", "dim2", "zzz" }, new Object[] { "val1", 4 });
            writeTS(iw, startTime + 5, new Object[] { "dim1", "aaa", "dim2", "xxx" }, new Object[] { "val1", 5 });
            writeTS(iw, startTime + 6, new Object[] { "dim1", "aaa", "dim2", "yyy" }, new Object[] { "val1", 6 });
            writeTS(iw, startTime + 7, new Object[] { "dim1", "bbb", "dim2", "zzz" }, new Object[] { "val1", 7 });
            writeTS(iw, startTime + 8, new Object[] { "dim1", "bbb", "dim2", "zzz" }, new Object[] { "val1", 8 });
        };
    }

    private void timeSeriesTestCase(
        TimeSeriesAggregationBuilder builder,
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalTimeSeries> verify,
        MappedFieldType... fieldTypes
    ) throws IOException {
        MappedFieldType[] newFieldTypes = new MappedFieldType[fieldTypes.length + 2];
        newFieldTypes[0] = TimeSeriesIdFieldMapper.FIELD_TYPE_BYTEREF;
        newFieldTypes[1] = new DateFieldMapper.DateFieldType("@timestamp");
        System.arraycopy(fieldTypes, 0, newFieldTypes, 2, fieldTypes.length);

        testCase(buildIndex, verify, new AggTestConfig(builder, newFieldTypes).withQuery(query));
    }

}
