/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AnyOperatorTestCase;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OperatorTestCase;
import org.elasticsearch.compute.operator.TestResultPageSinkOperator;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.junit.After;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class TimeSeriesSortedSourceOperatorTests extends AnyOperatorTestCase {

    private IndexReader reader;
    private final Directory directory = newDirectory();

    @After
    public void cleanup() throws IOException {
        IOUtils.close(reader, directory);
    }

    public void testSimple() {
        int numTimeSeries = 3;
        int numSamplesPerTS = 10;
        var ctx = driverContext();
        long timestampStart = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-01-01T00:00:00Z");
        var timeSeriesFactory = createTimeSeriesSourceOperator(writer -> {
            long timestamp = timestampStart;
            for (int i = 0; i < numSamplesPerTS; i++) {
                for (int j = 0; j < numTimeSeries; j++) {
                    String hostname = String.format("host-%02d", j);
                    writeTS(writer, timestamp, new Object[] { "hostname", hostname }, new Object[] { "voltage", j + 5 });
                }
                timestamp += 10_000;
                writer.commit();
            }
            return numTimeSeries * numSamplesPerTS;
        });

        List<Page> results = new ArrayList<>();
        var voltageField = new NumberFieldMapper.NumberFieldType("voltage", NumberFieldMapper.NumberType.LONG);
        OperatorTestCase.runDriver(
            new Driver(
                ctx,
                timeSeriesFactory.get(ctx),
                List.of(ValuesSourceReaderOperatorTests.factory(reader, voltageField, ElementType.LONG).get(ctx)),
                new TestResultPageSinkOperator(results::add),
                () -> {}
            )
        );
        OperatorTestCase.assertDriverContext(ctx);
        assertThat(results, hasSize(1));
        Page page = results.get(0);
        assertThat(page.getBlockCount(), equalTo(4));

        DocVector docVector = (DocVector) page.getBlock(0).asVector();
        assertThat(docVector.getPositionCount(), equalTo(numTimeSeries * numSamplesPerTS));

        IntVector tsidVector = (IntVector) page.getBlock(1).asVector();
        assertThat(tsidVector.getPositionCount(), equalTo(numTimeSeries * numSamplesPerTS));

        LongVector timestampVector = (LongVector) page.getBlock(2).asVector();
        assertThat(timestampVector.getPositionCount(), equalTo(numTimeSeries * numSamplesPerTS));

        LongVector voltageVector = (LongVector) page.getBlock(3).asVector();
        assertThat(voltageVector.getPositionCount(), equalTo(numTimeSeries * numSamplesPerTS));

        long timestampEnd = timestampStart + (10_000L * (numSamplesPerTS - 1));
        for (int i = 0; i < page.getPositionCount(); i++) {
            assertThat(docVector.shards().getInt(0), equalTo(0));
            assertThat(voltageVector.getLong(i), equalTo(5L + (i / numSamplesPerTS)));
            assertThat(tsidVector.getInt(i), equalTo(i / numSamplesPerTS));
            long expectedTimestamp = timestampEnd - ((i % numSamplesPerTS) * 10_000L);
            long actualTimestamp = timestampVector.getLong(i);
            assertThat(actualTimestamp, equalTo(expectedTimestamp));
        }
    }

    public void testRandom() {
        int numDocs = 1024;
        var ctx = driverContext();
        long timestampStart = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-01-01T00:00:00Z");
        var timeSeriesFactory = createTimeSeriesSourceOperator(writer -> {
            int commitEvery = 64;
            long timestamp = timestampStart;
            for (int i = 0; i < numDocs; i++) {
                String hostname = String.format("host-%02d", i % 20);
                int voltage = i % 5;
                writeTS(writer, timestamp, new Object[] { "hostname", hostname }, new Object[] { "voltage", voltage });
                if (i % commitEvery == 0) {
                    writer.commit();
                }
                timestamp += 10_000;
            }
            return numDocs;
        });
        List<Page> results = new ArrayList<>();

        var voltageField = new NumberFieldMapper.NumberFieldType("voltage", NumberFieldMapper.NumberType.LONG);
        OperatorTestCase.runDriver(
            new Driver(
                ctx,
                timeSeriesFactory.get(ctx),
                List.of(ValuesSourceReaderOperatorTests.factory(reader, voltageField, ElementType.LONG).get(ctx)),
                new TestResultPageSinkOperator(results::add),
                () -> {}
            )
        );
        OperatorTestCase.assertDriverContext(ctx);
        assertThat(results, hasSize(1));
        Page page = results.get(0);
        assertThat(page.getBlockCount(), equalTo(4));

        DocVector docVector = (DocVector) page.getBlock(0).asVector();
        assertThat(docVector.getPositionCount(), equalTo(numDocs));

        IntVector tsidVector = (IntVector) page.getBlock(1).asVector();
        assertThat(tsidVector.getPositionCount(), equalTo(numDocs));

        LongVector timestampVector = (LongVector) page.getBlock(2).asVector();
        assertThat(timestampVector.getPositionCount(), equalTo(numDocs));

        LongVector voltageVector = (LongVector) page.getBlock(3).asVector();
        assertThat(voltageVector.getPositionCount(), equalTo(numDocs));
        for (int i = 0; i < page.getBlockCount(); i++) {
            assertThat(docVector.shards().getInt(0), equalTo(0));
            assertThat(voltageVector.getLong(i), either(greaterThanOrEqualTo(0L)).or(lessThanOrEqualTo(4L)));
            assertThat(tsidVector.getInt(i), either(greaterThanOrEqualTo(0)).or(lessThan(20)));
            assertThat(timestampVector.getLong(i), greaterThanOrEqualTo(timestampStart));
        }
    }

    @Override
    protected Operator.OperatorFactory simple() {
        return createTimeSeriesSourceOperator(writer -> {
            long timestamp = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-01-01T00:00:00Z");
            writeTS(writer, timestamp, new Object[] { "hostname", "host-01" }, new Object[] { "voltage", 2 });
            return 1;
        });
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        return "TimeSeriesSortedSourceOperator[maxPageSize = 1, limit = 1]";
    }

    @Override
    protected String expectedToStringOfSimple() {
        return "Impl[maxPageSize=1, remainingDocs=1]";
    }

    TimeSeriesSortedSourceOperatorFactory createTimeSeriesSourceOperator(
        CheckedFunction<RandomIndexWriter, Integer, IOException> indexingLogic
    ) {
        int numDocs;
        Sort sort = new Sort(
            new SortField(TimeSeriesIdFieldMapper.NAME, SortField.Type.STRING, false),
            new SortedNumericSortField(DataStreamTimestampFieldMapper.DEFAULT_PATH, SortField.Type.LONG, true)
        );
        try (
            RandomIndexWriter writer = new RandomIndexWriter(
                random(),
                directory,
                newIndexWriterConfig().setIndexSort(sort).setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {

            numDocs = indexingLogic.apply(writer);
            reader = writer.getReader();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        var ctx = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
        Function<ShardContext, Query> queryFunction = c -> new MatchAllDocsQuery();
        // int maxPageSize = between(10, Math.max(10, numDocs));
        return TimeSeriesSortedSourceOperatorFactory.create(numDocs, numDocs, 1, List.of(ctx), queryFunction);
    }

    static void writeTS(RandomIndexWriter iw, long timestamp, Object[] dimensions, Object[] metrics) throws IOException {
        final List<IndexableField> fields = new ArrayList<>();
        fields.add(new SortedNumericDocValuesField(DataStreamTimestampFieldMapper.DEFAULT_PATH, timestamp));
        fields.add(new LongPoint(DataStreamTimestampFieldMapper.DEFAULT_PATH, timestamp));
        final TimeSeriesIdFieldMapper.TimeSeriesIdBuilder builder = new TimeSeriesIdFieldMapper.TimeSeriesIdBuilder(null);
        for (int i = 0; i < dimensions.length; i += 2) {
            if (dimensions[i + 1] instanceof Number n) {
                builder.addLong(dimensions[i].toString(), n.longValue());
            } else {
                builder.addString(dimensions[i].toString(), dimensions[i + 1].toString());
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
        fields.add(new SortedDocValuesField(TimeSeriesIdFieldMapper.NAME, builder.buildLegacyTsid().toBytesRef()));
        iw.addDocument(fields);
    }
}
