/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.aggregations.bucket;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.aggregations.AggregationsPlugin;
import org.elasticsearch.aggregations.bucket.timeseries.InternalTimeSeries;
import org.elasticsearch.aggregations.bucket.timeseries.TimeSeriesAggregationBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class TimeSeriesTsidHashCardinalityIT extends ESSingleNodeTestCase {
    private static final String START_TIME = "2021-01-01T00:00:00Z";
    private static final String END_TIME = "2021-12-31T23:59:59Z";
    private String beforeIndex, afterIndex;
    private long startTime, endTime;
    private int numDimensions, numTimeSeries;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(InternalSettingsPlugin.class);
        plugins.add(AggregationsPlugin.class);
        return plugins;
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        beforeIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        afterIndex = randomAlphaOfLength(12).toLowerCase(Locale.ROOT);
        startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(START_TIME);
        endTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(END_TIME);
        numTimeSeries = 500;
        // NOTE: we need to use few dimensions to be able to index documents in an index created before introducing TSID hashing
        numDimensions = randomIntBetween(10, 20);

        final Settings.Builder settings = indexSettings(1, 0).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of("dim_routing"))
            .put(
                IndexSettings.TIME_SERIES_START_TIME.getKey(),
                DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(Instant.ofEpochMilli(startTime).toEpochMilli())
            )
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), END_TIME);

        final XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties");
        mapping.startObject("@timestamp").field("type", "date").endObject();

        // Dimensions
        mapping.startObject("dim_routing").field("type", "keyword").field("time_series_dimension", true).endObject();
        for (int i = 1; i <= numDimensions; i++) {
            mapping.startObject("dim_" + i).field("type", "keyword").field("time_series_dimension", true).endObject();
        }
        // Metrics
        mapping.startObject("gauge").field("type", "double").field("time_series_metric", "gauge").endObject();
        mapping.endObject().endObject().endObject();
        assertAcked(
            indicesAdmin().prepareCreate(beforeIndex)
                .setSettings(
                    settings.put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersions.NEW_INDEXVERSION_FORMAT).build()
                )
                .setMapping(mapping),
            indicesAdmin().prepareCreate(afterIndex)
                .setSettings(
                    settings.put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersions.TIME_SERIES_ID_HASHING).build()
                )
                .setMapping(mapping)
        );

        final TimeSeriesDataset timeSeriesDataset = new TimeSeriesDataset();
        while (timeSeriesDataset.size() < numTimeSeries) {
            final Set<Dimension> dimensions = new TreeSet<>();
            for (int j = 0; j < numDimensions; j++) {
                if (randomIntBetween(1, 10) >= 2) { // NOTE: 20% chance the dimension field is empty
                    dimensions.add(new Dimension("dim_" + j, randomAlphaOfLength(12)));
                }
            }
            final TimeSeries ts = new TimeSeries(dimensions);
            if (timeSeriesDataset.exists(ts)) {
                continue;
            }
            for (int k = 0; k < randomIntBetween(10, 20); k++) {
                ts.addValue(randomLongBetween(startTime, endTime), randomDoubleBetween(100.0D, 200.0D, true));
            }
            timeSeriesDataset.add(ts);
        }

        final BulkRequestBuilder beforeBulkIndexRequest = client().prepareBulk();
        final BulkRequestBuilder afterBulkIndexRequest = client().prepareBulk();
        for (final TimeSeries ts : timeSeriesDataset) {
            for (final TimeSeriesValue timeSeriesValue : ts.values) {
                final XContentBuilder docSource = XContentFactory.jsonBuilder();
                docSource.startObject();
                docSource.field("dim_routing", "foo"); // Just to make sure we have at least one routing dimension
                for (int d = 0; d < numDimensions; d++) {
                    final Dimension dimension = ts.getDimension("dim_" + d);
                    if (dimension != null) {
                        docSource.field("dim_" + d, dimension.value);
                    }
                }
                docSource.field("@timestamp", timeSeriesValue.timestamp);
                docSource.field("gauge", timeSeriesValue.gauge);
                docSource.endObject();
                beforeBulkIndexRequest.add(prepareIndex(beforeIndex).setOpType(DocWriteRequest.OpType.CREATE).setSource(docSource));
                afterBulkIndexRequest.add(prepareIndex(afterIndex).setOpType(DocWriteRequest.OpType.CREATE).setSource(docSource));
            }
        }
        assertFalse(beforeBulkIndexRequest.get().hasFailures());
        assertFalse(afterBulkIndexRequest.get().hasFailures());
        assertEquals(RestStatus.OK, indicesAdmin().prepareRefresh(beforeIndex, afterIndex).get().getStatus());
    }

    public void testTimeSeriesNumberOfBuckets() {
        final SearchResponse searchBefore = client().prepareSearch(beforeIndex)
            .setSize(0)
            .addAggregation(new TimeSeriesAggregationBuilder("ts"))
            .get();
        final SearchResponse searchAfter = client().prepareSearch(afterIndex)
            .setSize(0)
            .addAggregation(new TimeSeriesAggregationBuilder("ts"))
            .get();
        try {
            final InternalTimeSeries beforeTimeSeries = searchBefore.getAggregations().get("ts");
            final InternalTimeSeries afterTimeSeries = searchAfter.getAggregations().get("ts");
            assertEquals(beforeTimeSeries.getBuckets().size(), afterTimeSeries.getBuckets().size());
        } finally {
            searchBefore.decRef();
            searchAfter.decRef();
        }
    }

    record Dimension(String name, String value) implements Comparable<Dimension> {

        @Override
        public String toString() {
            return "Dimension{" + "name='" + name + '\'' + ", value='" + value + '\'' + '}';
        }

        @Override
        public int compareTo(final Dimension that) {
            return Comparator.comparing(Dimension::name).thenComparing(Dimension::value).compare(this, that);
        }
    }

    record TimeSeriesValue(long timestamp, double gauge) {

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TimeSeriesValue that = (TimeSeriesValue) o;
            return timestamp == that.timestamp && Double.compare(gauge, that.gauge) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, gauge);
        }

        @Override
        public String toString() {
            return "TimeSeriesValue{" + "timestamp=" + timestamp + ", gauge=" + gauge + '}';
        }
    }

    static class TimeSeries {
        private final HashMap<String, Dimension> dimensions;
        private final Set<TimeSeriesValue> values;

        TimeSeries(final Set<Dimension> dimensions) {
            this.dimensions = new HashMap<>();
            for (final Dimension dimension : dimensions) {
                this.dimensions.put(dimension.name, dimension);
            }
            values = new TreeSet<>(Comparator.comparing(TimeSeriesValue::timestamp));
        }

        public void addValue(long timestamp, double gauge) {
            values.add(new TimeSeriesValue(timestamp, gauge));
        }

        public String id() {
            final StringBuilder sb = new StringBuilder();
            for (final Dimension dimension : dimensions.values().stream().sorted(Comparator.comparing(Dimension::name)).toList()) {
                sb.append(dimension.name()).append("=").append(dimension.value());
            }

            return sb.toString();
        }

        public Dimension getDimension(final String name) {
            return this.dimensions.get(name);
        }
    }

    static class TimeSeriesDataset implements Iterable<TimeSeries> {
        private final HashMap<String, TimeSeries> dataset;

        TimeSeriesDataset() {
            this.dataset = new HashMap<>();
        }

        public void add(final TimeSeries ts) {
            TimeSeries previous = dataset.put(ts.id(), ts);
            if (previous != null) {
                throw new IllegalArgumentException("Existing time series: " + ts.id());
            }
        }

        public boolean exists(final TimeSeries ts) {
            return dataset.containsKey(ts.id());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TimeSeriesDataset that = (TimeSeriesDataset) o;
            return Objects.equals(dataset, that.dataset);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataset);
        }

        @Override
        public String toString() {
            return "TimeSeriesDataset{" + "dataset=" + dataset + '}';
        }

        @Override
        public Iterator<TimeSeries> iterator() {
            return new TimeSeriesIterator(this.dataset.entrySet().iterator());
        }

        public int size() {
            return this.dataset.size();
        }

        record TimeSeriesIterator(Iterator<Map.Entry<String, TimeSeries>> it) implements Iterator<TimeSeries> {

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public TimeSeries next() {
                return it.next().getValue();
            }
        }
    }
}
