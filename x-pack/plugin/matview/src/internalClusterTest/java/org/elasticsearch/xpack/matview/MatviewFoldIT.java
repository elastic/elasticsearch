/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.matview;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * End to end: a view index receives partial {@code AVG(cpu) BY 1h bucket, hostname} contributions stored as
 * {@code aggregate_metric_double [sum, value_count]}; after a force merge it holds one row per (bucket, hostname)
 * group and the ES|QL aggregates over it are unchanged, since AVG over the field is already sum / value_count.
 * Contributions are either written directly or produced by the index-time fold of a source index.
 */
public class MatviewFoldIT extends AbstractEsqlIntegTestCase {

    private static final long HOUR = 3_600_000L;
    private static final int NUM_HOSTS = 10;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // InternalSettingsPlugin registers the test-only retention lease sync interval setting
        return CollectionUtils.appendToCopy(
            CollectionUtils.appendToCopy(
                CollectionUtils.appendToCopy(super.nodePlugins(), MatviewPlugin.class),
                AggregateMetricMapperPlugin.class
            ),
            InternalSettingsPlugin.class
        );
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false; // the view index needs the plugin's folding engine; only one engine factory may apply
    }

    /** A group of the view: the hour bucket and the host. */
    private record Group(long bucket, String host) implements Comparable<Group> {
        @Override
        public int compareTo(Group o) {
            int c = Long.compare(bucket, o.bucket);
            return c != 0 ? c : host.compareTo(o.host);
        }
    }

    /** Sums are compared after rounding, since the source sums raw values and the view sums partial sums. */
    private record Expected(double sum, long count) {
        private Expected {
            sum = Math.round(sum * 1e6) / 1e6;
        }
    }

    private static String randomHost() {
        return "host-" + between(0, NUM_HOSTS - 1);
    }

    private static IndexRequest contribution(String view, Group group, double sum, long count) {
        return new IndexRequest(view).source(
            Map.of("@timestamp", group.bucket(), "hostname", group.host(), "cpu", Map.of("sum", sum, "value_count", count))
        );
    }

    /** Pre-aggregated contributions written directly to the view (the "aggregate index you fill yourself" layer). */
    public void testMergeFoldsContributions() throws Exception {
        String view = "mv-cpu";
        createView(view);
        int numBuckets = between(1, 8);
        int numBatches = between(2, 6);
        Map<Group, double[]> expected = new TreeMap<>();
        long epoch = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);
        for (int batch = 0; batch < numBatches; batch++) {
            BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int b = 0; b < numBuckets; b++) {
                if (b > 0 && batch > 0 && randomBoolean()) {
                    continue; // not every batch touches every bucket, but the first bucket is always in every batch
                }
                Group group = new Group(epoch + b * HOUR, randomHost());
                double sum = randomDoubleBetween(0, 1000, true);
                int count = between(1, 20);
                bulk.add(contribution(view, group, sum, count));
                expected.merge(group, new double[] { sum, count }, (a, c) -> new double[] { a[0] + c[0], a[1] + c[1] });
            }
            BulkResponse response = bulk.get();
            assertFalse(response.buildFailureMessage(), response.hasFailures());
        }
        assertSame(expected, query(view));

        foldByForceMerge(view);
        assertThat("one row per group after folding", docCount(view), equalTo((long) expected.size()));
        assertSame(expected, query(view));
    }

    /**
     * All contributions in one segment: Lucene considers a lone segment without deletes merged already, so a force
     * merge to one segment only folds it because the folding policy merges such a segment with itself.
     */
    public void testLoneSegmentFolds() throws Exception {
        String view = "mv-cpu";
        createView(view);
        int numContributions = between(2, 50);
        long bucket = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        Map<Group, double[]> expected = new TreeMap<>();
        for (int i = 0; i < numContributions; i++) {
            Group group = new Group(bucket, randomHost());
            double s = randomDoubleBetween(0, 1000, true);
            int c = between(1, 20);
            bulk.add(contribution(view, group, s, c));
            expected.merge(group, new double[] { s, c }, (a, b) -> new double[] { a[0] + b[0], a[1] + b[1] });
        }
        BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        assertThat(indicesAdmin().prepareStats(view).get().getPrimaries().getSegments().getCount(), equalTo(1L));
        assertSame(expected, query(view));

        foldByForceMerge(view);
        assertThat("the lone segment folded", docCount(view), equalTo((long) expected.size()));
        assertSame(expected, query(view));
    }

    /**
     * The whole pipeline: raw documents go to a source index, the indexing listener folds every bulk into
     * contributions for the view, and a force merge of the view reduces them to one row per group. At every step
     * {@code SUM, COUNT, AVG BY TBUCKET(1h), hostname} over the view equals the same query over the source.
     */
    public void testIndexTimeFoldThenMerge() throws Exception {
        String view = "mv-cpu";
        String source = "cpu-source";
        createView(view);
        createSource(source, view);
        int numBuckets = between(1, 8);
        int numBatches = between(2, 6);
        long epoch = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);
        int rawDocs = 0;
        for (int batch = 0; batch < numBatches; batch++) {
            BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            int docs = between(1, 50);
            for (int i = 0; i < docs; i++) {
                long timestamp = epoch + randomLongBetween(0, numBuckets * HOUR - 1);
                bulk.add(sourceDoc(source, timestamp, randomHost(), randomDoubleBetween(0, 100, true)));
            }
            BulkResponse response = bulk.get();
            assertFalse(response.buildFailureMessage(), response.hasFailures());
            rawDocs += docs;
        }
        assertThat(docCount(source), equalTo((long) rawDocs));

        Map<Group, Expected> fromSource = awaitViewMatches(source, view);
        assertThat(docCount(view), greaterThan(0L));

        foldByForceMerge(view);
        assertThat("one row per group after folding", docCount(view), equalTo((long) fromSource.size()));
        assertThat(query(view), equalTo(fromSource));
    }

    /**
     * Scale: 10,000 raw documents spread uniformly over one week (168 hourly buckets) and ten hosts, indexed in bulks
     * of 500, with a fold half way through so that already folded representatives are folded again together with
     * fresh contributions. The view is checked against the source query and against an independently computed
     * per-group sum and count.
     */
    public void testWeekOfDataFoldsToOneRowPerGroup() throws Exception {
        weekOfData(10_000, 500, true);
    }

    /**
     * Scale: 100,000 raw documents over one week and ten hosts in bulks of 2,000, into a view sorted by hostname and
     * time, folded by merge-sorting. The view is queried as soon as all contributions have arrived and before any
     * merge, then folded by a force merge and queried again; both answers must equal the source.
     */
    public void testHundredThousandDocsSortedView() throws Exception {
        weekOfData(100_000, 2_000, false, true, "sorted");
    }

    /** Same data into a view without index sort: only the hash strategy can fold it. */
    public void testHundredThousandDocsUnsortedView() throws Exception {
        weekOfData(100_000, 2_000, false, false, "hash");
    }

    private void weekOfData(int numDocs, int bulkSize, boolean foldMidway) throws Exception {
        weekOfData(numDocs, bulkSize, foldMidway, randomBoolean(), "auto");
    }

    private void weekOfData(int numDocs, int bulkSize, boolean foldMidway, boolean sortedView, String strategy) throws Exception {
        String view = "mv-cpu";
        String source = "cpu-source";
        createView(view, sortedView, strategy);
        createSource(source, view);
        long epoch = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);
        long week = 7 * 24 * HOUR;
        Map<Group, double[]> expected = new TreeMap<>();
        int indexed = 0;
        while (indexed < numDocs) {
            BulkRequestBuilder bulk = client().prepareBulk();
            for (int i = 0; i < bulkSize; i++) {
                long timestamp = epoch + randomLongBetween(0, week - 1);
                String host = randomHost();
                double cpu = randomDoubleBetween(0, 100, true);
                bulk.add(sourceDoc(source, timestamp, host, cpu));
                expected.merge(
                    new Group(timestamp - timestamp % HOUR, host),
                    new double[] { cpu, 1 },
                    (a, c) -> new double[] { a[0] + c[0], a[1] + c[1] }
                );
            }
            BulkResponse response = bulk.get();
            assertFalse(response.buildFailureMessage(), response.hasFailures());
            indexed += bulkSize;
            if (foldMidway && indexed == numDocs / 2) {
                indicesAdmin().prepareRefresh(source).get();
                awaitViewMatches(source, view);
                foldByForceMerge(view);
                assertThat("one row per group after the intermediate fold", docCount(view), equalTo((long) expected.size()));
            }
        }
        indicesAdmin().prepareRefresh(source).get();
        assertThat(docCount(source), equalTo((long) numDocs));
        assertThat(expected.size(), lessThanOrEqualTo(7 * 24 * NUM_HOSTS));
        assertThat("far more groups than hours", expected.size(), greaterThan(7 * 24));
        logger.info(
            "--> source has [{}] live and [{}] deleted docs, the view [{}] docs before the check",
            docCount(source),
            indicesAdmin().prepareStats(source).get().getPrimaries().getDocs().getDeleted(),
            docCount(view)
        );

        // before any merge: the raw contributions already answer like the source
        Map<Group, Expected> fromSource = awaitViewMatches(source, view);
        assertSame(expected, fromSource);
        long contributions = docCount(view);
        assertThat("contributions outnumber groups before folding", contributions, greaterThan((long) expected.size()));
        logger.info("--> [{}] docs produced [{}] contributions in [{}] groups", numDocs, contributions, expected.size());

        foldByForceMerge(view);
        assertThat("one row per group after folding", docCount(view), equalTo((long) expected.size()));
        Map<Group, Expected> fromView = query(view);
        assertSame(expected, fromView);
        assertThat(fromView, equalTo(fromSource));
        assertThat("the source is unchanged", query(source), equalTo(fromSource));
    }

    private static IndexRequest sourceDoc(String source, long timestamp, String host, double cpu) {
        return new IndexRequest(source).source(Map.of("@timestamp", timestamp, "hostname", host, "cpu", cpu));
    }

    /** A view with a random layout; the strategy follows the sort. */
    private void createView(String view) throws IOException {
        createView(view, randomBoolean(), "auto");
    }

    /**
     * @param sorted   whether the view is sorted by hostname, then newest first: the layout the merge-sort strategy
     *                 needs; otherwise rows stay in arrival order and only the hash strategy applies
     * @param strategy the {@code index.matview.fold.strategy} to pin, or {@code auto}
     */
    private void createView(String view, boolean sorted, String strategy) throws IOException {
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "synthetic")
            // let the primary's retention lease catch up quickly so the whole history becomes foldable
            .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "1s")
            .putList(MatviewPlugin.FOLD_KEY.getKey(), "hostname:keyword", "@timestamp:long")
            .putList(MatviewPlugin.FOLD_REDUCE.getKey(), "cpu.sum:sum:double", "cpu.value_count:sum:int")
            .put(MatviewPlugin.FOLD_STRATEGY.getKey(), strategy);
        if (sorted) {
            settings.putList("index.sort.field", "hostname", "@timestamp").putList("index.sort.order", "asc", "desc");
        }
        assertAcked(indicesAdmin().prepareCreate(view).setSettings(settings).setMapping(viewMapping()));
    }

    /** A source index whose primaries fold every bulk into hourly, per-host contributions for the view. */
    private void createSource(String source, String view) {
        assertAcked(
            indicesAdmin().prepareCreate(source)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 3))
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(MatviewPlugin.SOURCE_TARGET.getKey(), view)
                        .put(MatviewPlugin.SOURCE_FIELD.getKey(), "cpu")
                        .putList(MatviewPlugin.SOURCE_GROUP.getKey(), "hostname")
                        .put(MatviewPlugin.SOURCE_INTERVAL.getKey(), "1h")
                )
                .setMapping("@timestamp", "type=date", "hostname", "type=keyword", "cpu", "type=double")
        );
    }

    /** Contributions arrive asynchronously; waits until the view answers like the source and returns that answer. */
    private Map<Group, Expected> awaitViewMatches(String source, String view) throws Exception {
        Map<Group, Expected> fromSource = query(source);
        assertBusy(() -> {
            indicesAdmin().prepareRefresh(view).get();
            assertThat(query(view), equalTo(fromSource));
        }, 60, TimeUnit.SECONDS);
        return fromSource;
    }

    /** Flushes, waits until retention releases every operation, and force merges the view down to one segment. */
    private void foldByForceMerge(String view) throws Exception {
        indicesAdmin().prepareFlush(view).get();
        long maxSeqNo = indicesAdmin().prepareStats(view).get().getShards()[0].getSeqNoStats().getMaxSeqNo();
        assertBusy(() -> {
            for (RetentionLease lease : indicesAdmin().prepareStats(view).get().getShards()[0].getRetentionLeaseStats()
                .retentionLeases()
                .leases()) {
                assertThat(lease.toString(), lease.retainingSequenceNumber(), greaterThan(maxSeqNo));
            }
        });
        indicesAdmin().prepareForceMerge(view).setMaxNumSegments(1).get();
        ensureGreen(view); // a failed merge fails the engine; fail fast instead of hanging on later calls
        indicesAdmin().prepareRefresh(view).get();
    }

    private long docCount(String index) {
        return indicesAdmin().prepareStats(index).get().getPrimaries().getDocs().getCount();
    }

    private static void assertSame(Map<Group, double[]> expected, Map<Group, Expected> actual) {
        assertThat(actual.keySet(), equalTo(expected.keySet()));
        for (Map.Entry<Group, double[]> e : expected.entrySet()) {
            Expected a = actual.get(e.getKey());
            assertThat("sum of " + e.getKey(), a.sum(), closeTo(e.getValue()[0], 1e-6 * Math.max(1, e.getValue()[0])));
            assertThat("count of " + e.getKey(), a.count(), equalTo((long) e.getValue()[1]));
        }
    }

    /** Runs the aggregation over the given index and returns sum and count per group, checking AVG agrees with them. */
    private Map<Group, Expected> query(String index) {
        Map<Group, Expected> result = new TreeMap<>();
        try (
            EsqlQueryResponse resp = run(
                "FROM "
                    + index
                    + " | STATS s = SUM(cpu), c = COUNT(cpu), a = AVG(cpu) BY b = TBUCKET(1h), hostname"
                    + " | SORT b, hostname | LIMIT 100000"
            )
        ) {
            List<String> columns = new ArrayList<>();
            resp.columns().forEach(c -> columns.add(c.name()));
            assertThat(columns, hasSize(5));
            for (Iterator<Iterator<Object>> rows = resp.values(); rows.hasNext();) {
                Iterator<Object> row = rows.next();
                double sum = ((Number) row.next()).doubleValue();
                long count = ((Number) row.next()).longValue();
                double avg = ((Number) row.next()).doubleValue();
                Object bucket = row.next();
                String host = row.next().toString();
                assertThat(avg, closeTo(sum / count, 1e-9 * Math.max(1, Math.abs(avg))));
                result.put(new Group(Instant.parse(bucket.toString()).toEpochMilli(), host), new Expected(sum, count));
            }
        }
        return result;
    }

    private static XContentBuilder viewMapping() throws IOException {
        XContentBuilder b = XContentFactory.jsonBuilder().startObject().startObject("properties");
        b.startObject("@timestamp").field("type", "date").endObject();
        b.startObject("hostname").field("type", "keyword").endObject();
        b.startObject("cpu")
            .field("type", "aggregate_metric_double")
            .array("metrics", "sum", "value_count")
            .field("default_metric", "sum")
            .endObject();
        return b.endObject().endObject();
    }
}
