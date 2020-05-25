/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.job;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig.CalendarInterval;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig.FixedInterval;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.number.OrderingComparison.greaterThan;

public class RollupIndexerIndexingTests extends AggregatorTestCase {
    private QueryShardContext queryShardContext;
    private IndexSettings settings;

    @Before
    private void setup() {
        settings = createIndexSettings();
        queryShardContext = new QueryShardContext(0, settings,
            BigArrays.NON_RECYCLING_INSTANCE, null, null, null, null, null,
                null, null, null, null, () -> 0L, null, null, () -> true, null);
    }

    public void testSimpleDateHisto() throws Exception {
        String rollupIndex = randomAlphaOfLength(10);
        String field = "the_histo";
        DateHistogramGroupConfig dateHistoConfig = new FixedInterval(field, new DateHistogramInterval("1ms"));
        RollupJobConfig job = createJob(rollupIndex, new GroupConfig(dateHistoConfig), Collections.emptyList());
        final List<Map<String, Object>> dataset = new ArrayList<>();
        dataset.addAll(
                Arrays.asList(
                        asMap("the_histo", 7L),
                        asMap("the_histo", 3L),
                        asMap("the_histo", 3L)
                )
        );
        executeTestCase(dataset, job, System.currentTimeMillis(), (resp) -> {
            assertThat(resp.size(), equalTo(2));
            IndexRequest request = resp.get(0);
            assertThat(request.index(), equalTo(rollupIndex));
            assertThat(request.sourceAsMap(), equalTo(
                    asMap(
                            "_rollup.version", 2,
                            "the_histo.date_histogram.timestamp", 3,
                            "the_histo.date_histogram.interval", "1ms",
                            "the_histo.date_histogram._count", 2,
                            "the_histo.date_histogram.time_zone", DateTimeZone.UTC.toString(),
                            "_rollup.id", job.getId()
                    )
            ));
            request = resp.get(1);
            assertThat(request.index(), equalTo(rollupIndex));
            assertThat(request.sourceAsMap(), equalTo(
                    asMap(
                            "_rollup.version", 2,
                            "the_histo.date_histogram.timestamp", 7,
                            "the_histo.date_histogram.interval", "1ms",
                            "the_histo.date_histogram._count", 1,
                            "the_histo.date_histogram.time_zone", DateTimeZone.UTC.toString(),
                            "_rollup.id", job.getId()
                    )
            ));
        });
    }

    public void testDateHistoAndMetrics() throws Exception {
        String rollupIndex = randomAlphaOfLength(10);
        String field = "the_histo";
        DateHistogramGroupConfig dateHistoConfig = new CalendarInterval(field, new DateHistogramInterval("1h"));
        MetricConfig config = new MetricConfig("counter", Arrays.asList("avg", "sum", "max", "min"));
        RollupJobConfig job = createJob(rollupIndex, new GroupConfig(dateHistoConfig), Collections.singletonList(config));
        final List<Map<String, Object>> dataset = new ArrayList<>();
        dataset.addAll(
                Arrays.asList(
                        asMap("the_histo", asLong("2015-03-31T03:00:00.000Z"), "counter", 10),
                        asMap("the_histo", asLong("2015-03-31T03:20:00.000Z"), "counter", 20),
                        asMap("the_histo", asLong("2015-03-31T03:40:00.000Z"), "counter", 20),
                        asMap("the_histo", asLong("2015-03-31T04:00:00.000Z"), "counter", 32),
                        asMap("the_histo", asLong("2015-03-31T04:20:00.000Z"), "counter", 54),
                        asMap("the_histo", asLong("2015-03-31T04:40:00.000Z"), "counter", 55),
                        asMap("the_histo", asLong("2015-03-31T05:00:00.000Z"), "counter", 55),
                        asMap("the_histo", asLong("2015-03-31T05:00:00.000Z"), "counter", 70),
                        asMap("the_histo", asLong("2015-03-31T05:20:00.000Z"), "counter", 70),
                        asMap("the_histo", asLong("2015-03-31T05:40:00.000Z"), "counter", 80),
                        asMap("the_histo", asLong("2015-03-31T06:00:00.000Z"), "counter", 80),
                        asMap("the_histo", asLong("2015-03-31T06:20:00.000Z"), "counter", 90),
                        asMap("the_histo", asLong("2015-03-31T06:40:00.000Z"), "counter", 100),
                        asMap("the_histo", asLong("2015-03-31T07:00:00.000Z"), "counter", 120),
                        asMap("the_histo", asLong("2015-03-31T07:20:00.000Z"), "counter", 120),
                        asMap("the_histo", asLong("2015-03-31T07:40:00.000Z"), "counter", 200)
                )
        );
        executeTestCase(dataset, job, System.currentTimeMillis(), (resp) -> {
            assertThat(resp.size(), equalTo(5));
            IndexRequest request = resp.get(0);
            assertThat(request.index(), equalTo(rollupIndex));
            assertThat(request.sourceAsMap(), equalTo(
                    asMap(
                            "_rollup.version", 2,
                            "the_histo.date_histogram.timestamp", asLong("2015-03-31T03:00:00.000Z"),
                            "the_histo.date_histogram.interval", "1h",
                            "the_histo.date_histogram._count", 3,
                            "counter.avg._count", 3.0,
                            "counter.avg.value", 50.0,
                            "counter.min.value", 10.0,
                            "counter.max.value", 20.0,
                            "counter.sum.value", 50.0,
                            "the_histo.date_histogram.time_zone", DateTimeZone.UTC.toString(),
                            "_rollup.id", job.getId()
                    )
            ));
            request = resp.get(1);
            assertThat(request.index(), equalTo(rollupIndex));
            assertThat(request.sourceAsMap(), equalTo(
                    asMap(
                            "_rollup.version", 2,
                            "the_histo.date_histogram.timestamp", asLong("2015-03-31T04:00:00.000Z"),
                            "the_histo.date_histogram.interval", "1h",
                            "the_histo.date_histogram._count", 3,
                            "counter.avg._count", 3.0,
                            "counter.avg.value", 141.0,
                            "counter.min.value", 32.0,
                            "counter.max.value", 55.0,
                            "counter.sum.value", 141.0,
                            "the_histo.date_histogram.time_zone", DateTimeZone.UTC.toString(),
                            "_rollup.id", job.getId()
                    )
            ));
            request = resp.get(2);
            assertThat(request.index(), equalTo(rollupIndex));
            assertThat(request.sourceAsMap(), equalTo(
                    asMap(
                            "_rollup.version", 2,
                            "the_histo.date_histogram.timestamp", asLong("2015-03-31T05:00:00.000Z"),
                            "the_histo.date_histogram.interval", "1h",
                            "the_histo.date_histogram._count", 4,
                            "counter.avg._count", 4.0,
                            "counter.avg.value", 275.0,
                            "counter.min.value", 55.0,
                            "counter.max.value", 80.0,
                            "counter.sum.value", 275.0,
                            "the_histo.date_histogram.time_zone", DateTimeZone.UTC.toString(),
                            "_rollup.id", job.getId()
                    )
            ));
            request = resp.get(3);
            assertThat(request.index(), equalTo(rollupIndex));
            assertThat(request.sourceAsMap(), equalTo(
                    asMap(
                            "_rollup.version", 2,
                            "the_histo.date_histogram.timestamp", asLong("2015-03-31T06:00:00.000Z"),
                            "the_histo.date_histogram.interval", "1h",
                            "the_histo.date_histogram._count", 3,
                            "counter.avg._count", 3.0,
                            "counter.avg.value", 270.0,
                            "counter.min.value", 80.0,
                            "counter.max.value", 100.0,
                            "counter.sum.value", 270.0,
                            "the_histo.date_histogram.time_zone", DateTimeZone.UTC.toString(),
                            "_rollup.id", job.getId()
                    )
            ));
            request = resp.get(4);
            assertThat(request.index(), equalTo(rollupIndex));
            assertThat(request.sourceAsMap(), equalTo(
                    asMap(
                            "_rollup.version", 2,
                            "the_histo.date_histogram.timestamp", asLong("2015-03-31T07:00:00.000Z"),
                            "the_histo.date_histogram.interval", "1h",
                            "the_histo.date_histogram._count", 3,
                            "counter.avg._count", 3.0,
                            "counter.avg.value", 440.0,
                            "counter.min.value", 120.0,
                            "counter.max.value", 200.0,
                            "counter.sum.value", 440.0,
                            "the_histo.date_histogram.time_zone", DateTimeZone.UTC.toString(),
                            "_rollup.id", job.getId()
                    )
            ));
        });
    }

    public void testSimpleDateHistoWithDelay() throws Exception {
        String rollupIndex = randomAlphaOfLengthBetween(5, 10);
        String field = "the_histo";
        DateHistogramGroupConfig dateHistoConfig =
            new FixedInterval(field, new DateHistogramInterval("1m"), new DateHistogramInterval("1h"), null);
        RollupJobConfig job = createJob(rollupIndex, new GroupConfig(dateHistoConfig), Collections.emptyList());
        final List<Map<String, Object>> dataset = new ArrayList<>();
        long now = System.currentTimeMillis();
        dataset.addAll(
                Arrays.asList(
                        asMap("the_histo", now - TimeValue.timeValueHours(5).getMillis()),
                        asMap("the_histo", now - TimeValue.timeValueHours(5).getMillis()),
                        asMap("the_histo", now - TimeValue.timeValueMinutes(75).getMillis()),
                        asMap("the_histo", now - TimeValue.timeValueMinutes(75).getMillis()),
                        asMap("the_histo", now - TimeValue.timeValueMinutes(61).getMillis()),
                        asMap("the_histo", now - TimeValue.timeValueHours(1).getMillis()),
                        asMap("the_histo", now - TimeValue.timeValueMinutes(10).getMillis()),
                        asMap("the_histo", now - TimeValue.timeValueMinutes(5).getMillis()),
                        asMap("the_histo", now - TimeValue.timeValueSeconds(1).getMillis()),
                        asMap("the_histo", now)
                )
        );
        final Rounding rounding = dateHistoConfig.createRounding();
        executeTestCase(dataset, job, now, (resp) -> {
            assertThat(resp.size(), equalTo(3));
            IndexRequest request = resp.get(0);
            assertThat(request.index(), equalTo(rollupIndex));
            assertThat(request.sourceAsMap(), equalTo(
                    asMap(
                            "_rollup.version", 2,
                            "the_histo.date_histogram.timestamp", rounding.round(now - TimeValue.timeValueHours(5).getMillis()),
                            "the_histo.date_histogram.interval", "1m",
                            "the_histo.date_histogram._count", 2,
                            "the_histo.date_histogram.time_zone", DateTimeZone.UTC.toString(),
                            "_rollup.id", job.getId()
                    )
            ));
            request = resp.get(1);
            assertThat(request.index(), equalTo(rollupIndex));
            assertThat(request.sourceAsMap(), equalTo(
                    asMap(
                            "_rollup.version", 2,
                            "the_histo.date_histogram.timestamp", rounding.round(now - TimeValue.timeValueMinutes(75).getMillis()),
                            "the_histo.date_histogram.interval", "1m",
                            "the_histo.date_histogram._count", 2,
                            "the_histo.date_histogram.time_zone", DateTimeZone.UTC.toString(),
                            "_rollup.id", job.getId()
                    )
            ));
            request = resp.get(2);
            assertThat(request.index(), equalTo(rollupIndex));
            assertThat(request.sourceAsMap(), equalTo(
                    asMap(
                            "_rollup.version", 2,
                            "the_histo.date_histogram.timestamp", rounding.round(now - TimeValue.timeValueMinutes(61).getMillis()),
                            "the_histo.date_histogram.interval", "1m",
                            "the_histo.date_histogram._count", 1,
                            "the_histo.date_histogram.time_zone", DateTimeZone.UTC.toString(),
                            "_rollup.id", job.getId()
                    )
            ));
        });
    }

    public void testSimpleDateHistoWithOverlappingDelay() throws Exception {
        String rollupIndex = randomAlphaOfLengthBetween(5, 10);
        String field = "the_histo";
        DateHistogramGroupConfig dateHistoConfig =
            new FixedInterval(field, new DateHistogramInterval("1h"), new DateHistogramInterval("15m"), null);
        RollupJobConfig job = createJob(rollupIndex, new GroupConfig(dateHistoConfig), Collections.emptyList());
        final List<Map<String, Object>> dataset = new ArrayList<>();
        long now = asLong("2015-04-01T10:30:00.000Z");
        dataset.addAll(
            Arrays.asList(
                asMap("the_histo", now - TimeValue.timeValueMinutes(135).getMillis()),
                asMap("the_histo", now - TimeValue.timeValueMinutes(120).getMillis()),
                asMap("the_histo", now - TimeValue.timeValueMinutes(105).getMillis()),
                asMap("the_histo", now - TimeValue.timeValueMinutes(90).getMillis()),
                asMap("the_histo", now - TimeValue.timeValueMinutes(75).getMillis()),
                asMap("the_histo", now - TimeValue.timeValueHours(1).getMillis()),
                asMap("the_histo", now - TimeValue.timeValueMinutes(45).getMillis()),
                asMap("the_histo", now - TimeValue.timeValueMinutes(30).getMillis()),
                asMap("the_histo", now - TimeValue.timeValueMinutes(15).getMillis()),
                asMap("the_histo", now)
            )
        );
        final Rounding rounding = dateHistoConfig.createRounding();
        executeTestCase(dataset, job, now, (resp) -> {
            assertThat(resp.size(), equalTo(2));
            IndexRequest request = resp.get(0);
            assertThat(request.index(), equalTo(rollupIndex));
            assertThat(request.sourceAsMap(), equalTo(
                asMap(
                    "_rollup.version", 2,
                    "the_histo.date_histogram.timestamp", rounding.round(now - TimeValue.timeValueHours(2).getMillis()),
                    "the_histo.date_histogram.interval", "1h",
                    "the_histo.date_histogram._count", 3,
                    "the_histo.date_histogram.time_zone", DateTimeZone.UTC.toString(),
                    "_rollup.id", job.getId()
                )
            ));
            request = resp.get(1);
            assertThat(request.index(), equalTo(rollupIndex));
            assertThat(request.sourceAsMap(), equalTo(
                asMap(
                    "_rollup.version", 2,
                    "the_histo.date_histogram.timestamp", rounding.round(now - TimeValue.timeValueHours(1).getMillis()),
                    "the_histo.date_histogram.interval", "1h",
                    "the_histo.date_histogram._count", 4,
                    "the_histo.date_histogram.time_zone", DateTimeZone.UTC.toString(),
                    "_rollup.id", job.getId()
                )
            ));
        });
    }

    public void testSimpleDateHistoWithTimeZone() throws Exception {
        final List<Map<String, Object>> dataset = new ArrayList<>();
        long now = asLong("2015-04-01T10:00:00.000Z");
        dataset.addAll(
                Arrays.asList(
                        asMap("the_histo", now - TimeValue.timeValueHours(10).getMillis()),
                        asMap("the_histo", now - TimeValue.timeValueHours(8).getMillis()),
                        asMap("the_histo", now - TimeValue.timeValueHours(6).getMillis()),
                        asMap("the_histo", now - TimeValue.timeValueMinutes(310).getMillis()),
                        asMap("the_histo", now - TimeValue.timeValueMinutes(305).getMillis()),
                        asMap("the_histo", now - TimeValue.timeValueMinutes(225).getMillis()),
                        asMap("the_histo", now)
                )
        );

        String timeZone = DateTimeZone.forOffsetHours(-3).getID();
        String rollupIndex = randomAlphaOfLengthBetween(5, 10);
        String field = "the_histo";
        DateHistogramGroupConfig dateHistoConfig =  new CalendarInterval(field, new DateHistogramInterval("1d"), null, timeZone);
        RollupJobConfig job = createJob(rollupIndex, new GroupConfig(dateHistoConfig), Collections.emptyList());

        executeTestCase(dataset, job, now, (resp) -> {
                    assertThat(resp.size(), equalTo(1));
                    IndexRequest request = resp.get(0);
                    assertThat(request.index(), equalTo(rollupIndex));
                    assertThat(request.sourceAsMap(), equalTo(
                            asMap(
                                    "_rollup.version", 2,
                                    "the_histo.date_histogram.timestamp", asLong("2015-03-31T03:00:00.000Z"),
                                    "the_histo.date_histogram.interval", "1d",
                                    "the_histo.date_histogram._count", 2,
                                    "the_histo.date_histogram.time_zone", timeZone.toString(),
                                    "_rollup.id", job.getId()
                            )
                    ));
                }
        );

        long nowPlusOneDay = now + TimeValue.timeValueHours(24).millis();
        executeTestCase(dataset, job, nowPlusOneDay, (resp) -> {
            assertThat(resp.size(), equalTo(2));
            IndexRequest request = resp.get(0);
            assertThat(request.index(), equalTo(rollupIndex));
            assertThat(request.sourceAsMap(), equalTo(
                    asMap(
                            "_rollup.version", 2,
                            "the_histo.date_histogram.timestamp", asLong("2015-03-31T03:00:00.000Z"),
                            "the_histo.date_histogram.interval", "1d",
                            "the_histo.date_histogram._count", 2,
                            "the_histo.date_histogram.time_zone", timeZone.toString(),
                            "_rollup.id", job.getId()
                    )
            ));
            request = resp.get(1);
            assertThat(request.index(), equalTo(rollupIndex));
            assertThat(request.sourceAsMap(), equalTo(
                    asMap(
                            "_rollup.version", 2,
                            "the_histo.date_histogram.timestamp", asLong("2015-04-01T03:00:00.000Z"),
                            "the_histo.date_histogram.interval", "1d",
                            "the_histo.date_histogram._count", 5,
                            "the_histo.date_histogram.time_zone", timeZone.toString(),
                            "_rollup.id", job.getId()
                    )
            ));
        });
    }

    public void testRandomizedDateHisto() throws Exception {
        String rollupIndex = randomAlphaOfLengthBetween(5, 10);

        String timestampField = "ts";
        String valueField = "the_avg";

        String timeInterval = randomIntBetween(2, 10) + randomFrom("h", "m");
        DateHistogramGroupConfig dateHistoConfig =
            new FixedInterval(timestampField, new DateHistogramInterval(timeInterval));
        MetricConfig metricConfig = new MetricConfig(valueField, Collections.singletonList("avg"));
        RollupJobConfig job = createJob(rollupIndex, new GroupConfig(dateHistoConfig), Collections.singletonList(metricConfig));

        final List<Map<String, Object>> dataset = new ArrayList<>();
        int numDocs = randomIntBetween(1,100);
        for (int i = 0; i < numDocs; i++) {
            // Make sure the timestamp is sufficiently in the past that we don't get bitten
            // by internal rounding, causing no docs to match
            long timestamp = new DateTime().minusDays(2).minusHours(randomIntBetween(11,100)).getMillis();
            dataset.add(asMap(timestampField, timestamp, valueField, randomLongBetween(1, 100)));
        }
        executeTestCase(dataset, job, System.currentTimeMillis(), (resp) -> {
            assertThat(resp.size(), greaterThan(0));
            for (IndexRequest request : resp) {
                assertThat(request.index(), equalTo(rollupIndex));

                Map<String, Object> source = request.sourceAsMap();

                assertThat(source.get("_rollup.version"), equalTo(2));
                assertThat(source.get("ts.date_histogram.interval"), equalTo(timeInterval.toString()));
                assertNotNull(source.get("the_avg.avg._count"));
                assertNotNull(source.get("the_avg.avg.value"));
                assertNotNull(source.get("ts.date_histogram._count"));
                assertNotNull(source.get("ts.date_histogram.interval"));
                assertNotNull(source.get("ts.date_histogram.timestamp"));
            }
        });
    }

    private RollupJobConfig createJob(String rollupIndex, GroupConfig groupConfig, List<MetricConfig> metricConfigs) {
        return new RollupJobConfig(randomAlphaOfLength(10), randomAlphaOfLength(10), rollupIndex, ConfigTestHelpers.randomCron(),
            randomIntBetween(1, 100), groupConfig, metricConfigs, ConfigTestHelpers.randomTimeout(random()));
    }

    static Map<String, Object> asMap(Object... fields) {
        assert fields.length % 2 == 0;
        final Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < fields.length; i+=2) {
            String field = (String) fields[i];
            map.put(field, fields[i+1]);
        }
        return map;
    }

    private static long asLong(String dateTime) {
        return DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(dateTime);
    }

    /**
     * Executes a rollup test case
     * @param docs The docs to index in the source
     * @param config The rollup job to execute
     * @param now The current time in milliseconds
     * @param rollupConsumer The consumer that checks the created rollup documents for the job
     */
    private void executeTestCase(List<Map<String, Object>> docs, RollupJobConfig config, long now,
                                 Consumer<List<IndexRequest>> rollupConsumer) throws Exception {
        Map<String, MappedFieldType> fieldTypeLookup = createFieldTypes(config);
        Directory dir = index(docs, fieldTypeLookup);
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);
        String dateHistoField = config.getGroupConfig().getDateHistogram().getField();
        final ThreadPool threadPool = new TestThreadPool(getTestName());

        try {
            RollupJob job = new RollupJob(config, Collections.emptyMap());
            final SyncRollupIndexer action = new SyncRollupIndexer(threadPool, ThreadPool.Names.GENERIC, job, searcher,
                    fieldTypeLookup.values().toArray(new MappedFieldType[0]), fieldTypeLookup.get(dateHistoField));
            rollupConsumer.accept(action.triggerAndWaitForCompletion(now));
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
            reader.close();
            dir.close();
        }
    }

    /**
     * Creates {@link MappedFieldType} from the provided <code>job</code>.
     * For simplicity all numbers are considered as longs.
     *
     * @return A map containing all created field types accessible by their names
     */
    private Map<String, MappedFieldType> createFieldTypes(RollupJobConfig job) {
        Map<String, MappedFieldType> fieldTypes = new HashMap<>();
        MappedFieldType fieldType = new DateFieldMapper.Builder(job.getGroupConfig().getDateHistogram().getField())
                .format(randomFrom("basic_date", "date_optional_time", "epoch_second"))
                .locale(Locale.ROOT)
                .build(new Mapper.BuilderContext(settings.getSettings(), new ContentPath(0)))
                .fieldType();
        fieldTypes.put(fieldType.name(), fieldType);

        if (job.getGroupConfig().getHistogram() != null) {
            for (String field : job.getGroupConfig().getHistogram().getFields()) {
                MappedFieldType ft = new NumberFieldMapper.Builder(field, NumberFieldMapper.NumberType.LONG)
                        .build(new Mapper.BuilderContext(settings.getSettings(), new ContentPath(0)))
                        .fieldType();
                fieldTypes.put(ft.name(), ft);
            }
        }

        if (job.getGroupConfig().getTerms() != null) {
            for (String field : job.getGroupConfig().getTerms().getFields()) {
                MappedFieldType ft = new KeywordFieldMapper.Builder(field)
                        .build(new Mapper.BuilderContext(settings.getSettings(), new ContentPath(0)))
                        .fieldType();
                fieldTypes.put(ft.name(), ft);
            }
        }

        if (job.getMetricsConfig() != null) {
            for (MetricConfig metric : job.getMetricsConfig()) {
                MappedFieldType ft = new NumberFieldMapper.Builder(metric.getField(), NumberFieldMapper.NumberType.LONG)
                        .build(new Mapper.BuilderContext(settings.getSettings(), new ContentPath(0)))
                        .fieldType();
                fieldTypes.put(ft.name(), ft);
            }
        }
        return fieldTypes;
    }

    @SuppressWarnings("unchecked")
    private Directory index(List<Map<String, Object>> docs, Map<String, MappedFieldType> fieldTypeLookup) throws IOException {
        Directory directory = LuceneTestCase.newDirectory();
        IndexWriterConfig config = LuceneTestCase.newIndexWriterConfig(LuceneTestCase.random(), new MockAnalyzer(LuceneTestCase.random()));
        try (RandomIndexWriter indexWriter = new RandomIndexWriter(LuceneTestCase.random(), directory, config)) {
            Document luceneDoc = new Document();
            for (Map<String, Object> doc : docs) {
                luceneDoc.clear();
                for (Map.Entry<String, Object> entry : doc.entrySet()) {
                    final String name = entry.getKey();
                    final Object value = entry.getValue();
                    MappedFieldType ft = fieldTypeLookup.get(name);
                    Collection<Object> values;
                    if (value instanceof Collection) {
                        values = (Collection<Object>) value;
                    } else {
                        values = Collections.singletonList(value);
                    }
                    for (Object obj : values) {
                        if (ft instanceof KeywordFieldMapper.KeywordFieldType) {
                            luceneDoc.add(new SortedSetDocValuesField(name, new BytesRef(obj.toString())));
                        } else if (ft instanceof DateFieldMapper.DateFieldType || ft instanceof NumberFieldMapper.NumberFieldType) {
                            assert obj instanceof Number;
                            // Force all numbers to longs
                            long longValue = ((Number) value).longValue();
                            luceneDoc.add(new SortedNumericDocValuesField(name, longValue));
                            luceneDoc.add(new LongPoint(name, longValue));
                        }
                    }
                }
                indexWriter.addDocument(luceneDoc);

            }
            indexWriter.commit();
        }
        return directory;
    }

    class SyncRollupIndexer extends RollupIndexer {
        private final IndexSearcher searcher;
        private final MappedFieldType[] fieldTypes;
        private final MappedFieldType timestampField;
        private final List<IndexRequest> documents = new ArrayList<>();
        private final CountDownLatch latch = new CountDownLatch(1);
        private Exception exc;

        SyncRollupIndexer(ThreadPool threadPool, String executorName, RollupJob job, IndexSearcher searcher,
                          MappedFieldType[] fieldTypes, MappedFieldType timestampField) {
            super(threadPool, executorName, job, new AtomicReference<>(IndexerState.STARTED), null);
            this.searcher = searcher;
            this.fieldTypes = fieldTypes;
            this.timestampField = timestampField;
        }

        @Override
        protected void onFinish(ActionListener<Void> listener) {
            latch.countDown();
            listener.onResponse(null);
        }

        @Override
        protected void onAbort() {
            assert false : "onAbort should not be called";
        }

        @Override
        protected void onFailure(Exception e) {
            latch.countDown();
            exc = e;
        }

        @Override
        protected void doNextSearch(SearchRequest request, ActionListener<SearchResponse> listener) {
            assertNotNull(request.source());

            // extract query
            assertThat(request.source().query(), instanceOf(RangeQueryBuilder.class));
            RangeQueryBuilder range = (RangeQueryBuilder) request.source().query();
            final ZoneId timeZone = range.timeZone() != null ? ZoneId.of(range.timeZone()) : null;
            Query query = timestampField.rangeQuery(range.from(), range.to(), range.includeLower(), range.includeUpper(),
                    null, timeZone, DateFormatter.forPattern(range.format()).toDateMathParser(), queryShardContext);

            // extract composite agg
            assertThat(request.source().aggregations().getAggregatorFactories().size(), equalTo(1));
            assertThat(request.source().aggregations().getAggregatorFactories().iterator().next(),
                    instanceOf(CompositeAggregationBuilder.class));
            CompositeAggregationBuilder aggBuilder =
                    (CompositeAggregationBuilder) request.source().aggregations().getAggregatorFactories().iterator().next();

            CompositeAggregation result = null;
            try {
                result = search(searcher, query, aggBuilder, fieldTypes);
            } catch (IOException e) {
                listener.onFailure(e);
            }
            SearchResponseSections sections = new SearchResponseSections(null, new Aggregations(Collections.singletonList(result)),
                    null, false, null, null, 1);
            SearchResponse response = new SearchResponse(sections, null, 1, 1,
                    0, 0, ShardSearchFailure.EMPTY_ARRAY, null);
            listener.onResponse(response);
        }

        @Override
        protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> listener) {
            for (DocWriteRequest<?> indexRequest : request.requests()) {
                if (indexRequest.getClass() == IndexRequest.class) {
                    documents.add(((IndexRequest) indexRequest));
                } else {
                    listener.onFailure(new IllegalStateException("invalid bulk request"));
                }
            }
            listener.onResponse(new BulkResponse(new BulkItemResponse[0], 0));
        }

        @Override
        protected void doSaveState(IndexerState state, Map<String, Object> position, Runnable next) {
            assert state == IndexerState.INDEXING || state == IndexerState.STARTED || state == IndexerState.STOPPED;
            next.run();
        }

        public List<IndexRequest> triggerAndWaitForCompletion(long now) throws Exception {
            assertTrue(maybeTriggerAsyncJob(now));
            try {
                latch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            if (exc != null) {
                throw exc;
            }
            assertThat(latch.getCount(), equalTo(0L));
            return documents;

        }
    }
}
