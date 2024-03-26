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
import org.elasticsearch.common.Randomness;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.RateLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AnyOperatorTestCase;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OperatorTestCase;
import org.elasticsearch.compute.operator.OrdinalsGroupingOperator;
import org.elasticsearch.compute.operator.TestResultPageSinkOperator;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.BlockDocValuesReader;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.junit.After;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
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
        long timestampStart = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-01-01T00:00:00Z");
        int maxPageSize = between(1, 1024);
        List<Page> results = runDriver(1024, maxPageSize, randomBoolean(), numTimeSeries, numSamplesPerTS, timestampStart);
        // for now we emit at most one time series each page
        int offset = 0;
        for (Page page : results) {
            assertThat(page.getBlockCount(), equalTo(5));
            DocVector docVector = (DocVector) page.getBlock(0).asVector();
            IntVector tsidVector = (IntVector) page.getBlock(1).asVector();
            LongVector timestampVector = (LongVector) page.getBlock(2).asVector();
            LongVector voltageVector = (LongVector) page.getBlock(3).asVector();
            BytesRefVector hostnameVector = (BytesRefVector) page.getBlock(4).asVector();
            for (int i = 0; i < page.getPositionCount(); i++) {
                int expectedTsidOrd = offset / numSamplesPerTS;
                String expectedHostname = String.format(Locale.ROOT, "host-%02d", expectedTsidOrd);
                long expectedVoltage = 5L + expectedTsidOrd;
                int sampleIndex = offset - expectedTsidOrd * numSamplesPerTS;
                long expectedTimestamp = timestampStart + ((numSamplesPerTS - sampleIndex - 1) * 10_000L);
                assertThat(docVector.shards().getInt(i), equalTo(0));
                assertThat(voltageVector.getLong(i), equalTo(expectedVoltage));
                assertThat(hostnameVector.getBytesRef(i, new BytesRef()).utf8ToString(), equalTo(expectedHostname));
                assertThat(tsidVector.getInt(i), equalTo(expectedTsidOrd));
                assertThat(timestampVector.getLong(i), equalTo(expectedTimestamp));
                offset++;
            }
        }
    }

    public void testLimit() {
        int numTimeSeries = 3;
        int numSamplesPerTS = 10;
        int limit = 1;
        long timestampStart = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-01-01T00:00:00Z");
        List<Page> results = runDriver(limit, randomIntBetween(1, 1024), randomBoolean(), numTimeSeries, numSamplesPerTS, timestampStart);
        assertThat(results, hasSize(1));
        Page page = results.get(0);
        assertThat(page.getBlockCount(), equalTo(5));

        DocVector docVector = (DocVector) page.getBlock(0).asVector();
        assertThat(docVector.getPositionCount(), equalTo(limit));

        IntVector tsidVector = (IntVector) page.getBlock(1).asVector();
        assertThat(tsidVector.getPositionCount(), equalTo(limit));

        LongVector timestampVector = (LongVector) page.getBlock(2).asVector();
        assertThat(timestampVector.getPositionCount(), equalTo(limit));

        LongVector voltageVector = (LongVector) page.getBlock(3).asVector();
        assertThat(voltageVector.getPositionCount(), equalTo(limit));

        BytesRefVector hostnameVector = (BytesRefVector) page.getBlock(4).asVector();
        assertThat(hostnameVector.getPositionCount(), equalTo(limit));

        assertThat(docVector.shards().getInt(0), equalTo(0));
        assertThat(voltageVector.getLong(0), equalTo(5L));
        assertThat(hostnameVector.getBytesRef(0, new BytesRef()).utf8ToString(), equalTo("host-00"));
        assertThat(tsidVector.getInt(0), equalTo(0));
        assertThat(timestampVector.getLong(0), equalTo(timestampStart + ((numSamplesPerTS - 1) * 10_000L)));
    }

    public void testRandom() {
        record Doc(int host, long timestamp, long metric) {}
        int numDocs = between(1, 5000);
        List<Doc> docs = new ArrayList<>();
        Map<Integer, Long> timestamps = new HashMap<>();
        long t0 = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-01-01T00:00:00Z");
        for (int i = 0; i < numDocs; i++) {
            int tsid = randomIntBetween(0, 9);
            long timestamp = timestamps.compute(tsid, (k, curr) -> {
                long t = curr != null ? curr : t0;
                return t + randomIntBetween(1, 5000);
            });
            docs.add(new Doc(tsid, timestamp, randomIntBetween(1, 10000)));
        }
        int maxPageSize = between(1, 1024);
        int limit = randomBoolean() ? between(1, 100000) : Integer.MAX_VALUE;
        var timeSeriesFactory = createTimeSeriesSourceOperator(limit, maxPageSize, randomBoolean(), writer -> {
            Randomness.shuffle(docs);
            for (Doc doc : docs) {
                writeTS(writer, doc.timestamp, new Object[] { "hostname", "h" + doc.host }, new Object[] { "metric", doc.metric });
            }
            return docs.size();
        });
        DriverContext driverContext = driverContext();
        List<Page> results = new ArrayList<>();
        var metricField = new NumberFieldMapper.NumberFieldType("metric", NumberFieldMapper.NumberType.LONG);
        OperatorTestCase.runDriver(
            new Driver(
                driverContext,
                timeSeriesFactory.get(driverContext),
                List.of(ValuesSourceReaderOperatorTests.factory(reader, metricField, ElementType.LONG).get(driverContext)),
                new TestResultPageSinkOperator(results::add),
                () -> {}
            )
        );
        docs.sort(Comparator.comparing(Doc::host).thenComparing(Comparator.comparingLong(Doc::timestamp).reversed()));
        Map<Integer, Integer> hostToTsidOrd = new HashMap<>();
        timestamps.keySet().stream().sorted().forEach(n -> hostToTsidOrd.put(n, hostToTsidOrd.size()));
        int offset = 0;
        for (int p = 0; p < results.size(); p++) {
            Page page = results.get(p);
            if (p < results.size() - 1) {
                assertThat(page.getPositionCount(), equalTo(maxPageSize));
            } else {
                assertThat(page.getPositionCount(), lessThanOrEqualTo(limit));
                assertThat(page.getPositionCount(), lessThanOrEqualTo(maxPageSize));
            }
            assertThat(page.getBlockCount(), equalTo(4));
            DocVector docVector = (DocVector) page.getBlock(0).asVector();
            IntVector tsidVector = (IntVector) page.getBlock(1).asVector();
            LongVector timestampVector = (LongVector) page.getBlock(2).asVector();
            LongVector metricVector = (LongVector) page.getBlock(3).asVector();
            for (int i = 0; i < page.getPositionCount(); i++) {
                Doc doc = docs.get(offset);
                offset++;
                assertThat(docVector.shards().getInt(0), equalTo(0));
                assertThat(tsidVector.getInt(i), equalTo(hostToTsidOrd.get(doc.host)));
                assertThat(timestampVector.getLong(i), equalTo(doc.timestamp));
                assertThat(metricVector.getLong(i), equalTo(doc.metric));
            }
        }
        assertThat(offset, equalTo(Math.min(limit, numDocs)));
    }

    public void testBasicRate() {
        long[] v1 = { 1, 1, 3, 0, 2, 9, 21, 3, 7, 7, 9, 12 };
        long[] t1 = { 1, 5, 11, 20, 21, 59, 88, 91, 92, 97, 99, 112 };

        long[] v2 = { 7, 2, 0, 11, 24, 0, 4, 1, 10, 2 };
        long[] t2 = { 1, 2, 4, 5, 6, 8, 10, 11, 12, 14 };

        long[] v3 = { 0, 1, 0, 1, 1, 4, 2, 2, 2, 2, 3, 5, 5 };
        long[] t3 = { 2, 3, 5, 7, 8, 9, 10, 12, 14, 15, 18, 20, 22 };
        List<Pod> pods = List.of(new Pod("p1", t1, v1), new Pod("p2", t2, v2), new Pod("p3", t3, v3));
        long unit = between(1, 5);
        Map<String, Double> actualRates = runRateTest(pods, TimeValue.timeValueMillis(unit));
        assertThat(actualRates, equalTo(Map.of("p1", 35.0 * unit / 111.0, "p2", 42.0 * unit / 13.0, "p3", 10.0 * unit / 20.0)));
    }

    public void testRandomRate() {
        int numPods = between(1, 10);
        List<Pod> pods = new ArrayList<>();
        Map<String, Double> expectedRates = new HashMap<>();
        TimeValue unit = TimeValue.timeValueSeconds(1);
        for (int p = 0; p < numPods; p++) {
            int numValues = between(2, 100);
            long[] values = new long[numValues];
            long[] times = new long[numValues];
            long t = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-01-01T00:00:00Z");
            for (int i = 0; i < numValues; i++) {
                values[i] = randomIntBetween(0, 100);
                t += TimeValue.timeValueSeconds(between(1, 10)).millis();
                times[i] = t;
            }
            Pod pod = new Pod("p" + p, times, values);
            pods.add(pod);
            if (numValues == 1) {
                expectedRates.put(pod.name, null);
            } else {
                expectedRates.put(pod.name, pod.expectedRate(unit));
            }
        }
        Map<String, Double> actualRates = runRateTest(pods, unit);
        assertThat(actualRates, equalTo(expectedRates));
    }

    record Pod(String name, long[] times, long[] values) {
        Pod {
            assert times.length == values.length : times.length + "!=" + values.length;
        }

        double expectedRate(TimeValue unit) {
            double dv = 0;
            for (int i = 0; i < values.length - 1; i++) {
                if (values[i + 1] < values[i]) {
                    dv += values[i];
                }
            }
            dv += (values[values.length - 1] - values[0]);
            long dt = times[times.length - 1] - times[0];
            return (dv * unit.millis()) / dt;
        }
    }

    Map<String, Double> runRateTest(List<Pod> pods, TimeValue unit) {
        long unitInMillis = unit.millis();
        record Doc(String pod, long timestamp, long requests) {

        }
        var sourceOperatorFactory = createTimeSeriesSourceOperator(Integer.MAX_VALUE, between(1, 100), randomBoolean(), writer -> {
            List<Doc> docs = new ArrayList<>();
            for (Pod pod : pods) {
                for (int i = 0; i < pod.times.length; i++) {
                    docs.add(new Doc(pod.name, pod.times[i], pod.values[i]));
                }
            }
            Randomness.shuffle(docs);
            for (Doc doc : docs) {
                writeTS(writer, doc.timestamp, new Object[] { "pod", doc.pod }, new Object[] { "requests", doc.requests });
            }
            return docs.size();
        });
        var ctx = driverContext();
        HashAggregationOperator finalHash = new HashAggregationOperator(
            List.of(new RateLongAggregatorFunctionSupplier(List.of(1, 2, 3), unitInMillis).groupingAggregatorFactory(AggregatorMode.FINAL)),
            () -> BlockHash.build(
                List.of(new HashAggregationOperator.GroupSpec(0, ElementType.BYTES_REF)),
                ctx.blockFactory(),
                randomIntBetween(1, 1000),
                randomBoolean()
            ),
            ctx
        );
        List<Page> results = new ArrayList<>();
        var requestsField = new NumberFieldMapper.NumberFieldType("requests", NumberFieldMapper.NumberType.LONG);
        var podField = new KeywordFieldMapper.KeywordFieldType("pod");
        if (randomBoolean()) {
            HashAggregationOperator initialHash = new HashAggregationOperator(
                List.of(
                    new RateLongAggregatorFunctionSupplier(List.of(4, 2), unitInMillis).groupingAggregatorFactory(AggregatorMode.INITIAL)
                ),
                () -> BlockHash.build(
                    List.of(new HashAggregationOperator.GroupSpec(3, ElementType.BYTES_REF)),
                    ctx.blockFactory(),
                    randomIntBetween(1, 1000),
                    randomBoolean()
                ),
                ctx
            );
            OperatorTestCase.runDriver(
                new Driver(
                    ctx,
                    sourceOperatorFactory.get(ctx),
                    List.of(
                        ValuesSourceReaderOperatorTests.factory(reader, podField, ElementType.BYTES_REF).get(ctx),
                        ValuesSourceReaderOperatorTests.factory(reader, requestsField, ElementType.LONG).get(ctx),
                        initialHash,
                        finalHash
                    ),
                    new TestResultPageSinkOperator(results::add),
                    () -> {}
                )
            );
        } else {
            var blockLoader = new BlockDocValuesReader.BytesRefsFromOrdsBlockLoader("pod");
            var shardContext = new ValuesSourceReaderOperator.ShardContext(reader, () -> SourceLoader.FROM_STORED_SOURCE);
            var ordinalGrouping = new OrdinalsGroupingOperator(
                shardIdx -> blockLoader,
                List.of(shardContext),
                ElementType.BYTES_REF,
                0,
                "pod",
                List.of(
                    new RateLongAggregatorFunctionSupplier(List.of(3, 2), unitInMillis).groupingAggregatorFactory(AggregatorMode.INITIAL)
                ),
                randomIntBetween(1, 1000),
                ctx
            );
            OperatorTestCase.runDriver(
                new Driver(
                    ctx,
                    sourceOperatorFactory.get(ctx),
                    List.of(
                        ValuesSourceReaderOperatorTests.factory(reader, requestsField, ElementType.LONG).get(ctx),
                        ordinalGrouping,
                        finalHash
                    ),
                    new TestResultPageSinkOperator(results::add),
                    () -> {}
                )
            );
        }
        Map<String, Double> rates = new HashMap<>();
        for (Page result : results) {
            BytesRefBlock keysBlock = result.getBlock(0);
            DoubleBlock ratesBlock = result.getBlock(1);
            for (int i = 0; i < result.getPositionCount(); i++) {
                rates.put(keysBlock.getBytesRef(i, new BytesRef()).utf8ToString(), ratesBlock.getDouble(i));
            }
            result.releaseBlocks();
        }
        return rates;
    }

    @Override
    protected Operator.OperatorFactory simple() {
        return createTimeSeriesSourceOperator(1, 1, false, writer -> {
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

    List<Page> runDriver(int limit, int maxPageSize, boolean forceMerge, int numTimeSeries, int numSamplesPerTS, long timestampStart) {
        var ctx = driverContext();
        var timeSeriesFactory = createTimeSeriesSourceOperator(limit, maxPageSize, forceMerge, writer -> {
            long timestamp = timestampStart;
            for (int i = 0; i < numSamplesPerTS; i++) {
                for (int j = 0; j < numTimeSeries; j++) {
                    String hostname = String.format(Locale.ROOT, "host-%02d", j);
                    writeTS(writer, timestamp, new Object[] { "hostname", hostname }, new Object[] { "voltage", j + 5 });
                }
                timestamp += 10_000;
                writer.commit();
            }
            return numTimeSeries * numSamplesPerTS;
        });

        List<Page> results = new ArrayList<>();
        var voltageField = new NumberFieldMapper.NumberFieldType("voltage", NumberFieldMapper.NumberType.LONG);
        var hostnameField = new KeywordFieldMapper.KeywordFieldType("hostname");
        OperatorTestCase.runDriver(
            new Driver(
                ctx,
                timeSeriesFactory.get(ctx),
                List.of(
                    ValuesSourceReaderOperatorTests.factory(reader, voltageField, ElementType.LONG).get(ctx),
                    ValuesSourceReaderOperatorTests.factory(reader, hostnameField, ElementType.BYTES_REF).get(ctx)
                ),
                new TestResultPageSinkOperator(results::add),
                () -> {}
            )
        );
        OperatorTestCase.assertDriverContext(ctx);
        for (Page result : results) {
            assertThat(result.getPositionCount(), lessThanOrEqualTo(maxPageSize));
            assertThat(result.getPositionCount(), lessThanOrEqualTo(limit));
        }
        return results;
    }

    TimeSeriesSortedSourceOperatorFactory createTimeSeriesSourceOperator(
        int limit,
        int maxPageSize,
        boolean forceMerge,
        CheckedFunction<RandomIndexWriter, Integer, IOException> indexingLogic
    ) {
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

            int numDocs = indexingLogic.apply(writer);
            if (forceMerge) {
                writer.forceMerge(1);
            }
            reader = writer.getReader();
            assertThat(reader.numDocs(), equalTo(numDocs));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        var ctx = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
        Function<ShardContext, Query> queryFunction = c -> new MatchAllDocsQuery();
        return TimeSeriesSortedSourceOperatorFactory.create(limit, maxPageSize, 1, List.of(ctx), queryFunction);
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
        // Use legacy tsid to make tests easier to understand:
        fields.add(new SortedDocValuesField(TimeSeriesIdFieldMapper.NAME, builder.buildLegacyTsid().toBytesRef()));
        iw.addDocument(fields);
    }
}
