/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.v2;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DocCountFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat.TimeSeriesIdDocValueFormat;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionDateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.action.RollupShardStatus;
import org.elasticsearch.xpack.core.rollup.action.RollupShardStatus.Status;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;
import org.elasticsearch.xpack.rollup.v2.indexer.metrics.LeafMetricField;
import org.elasticsearch.xpack.rollup.v2.indexer.metrics.MetricCollector;
import org.elasticsearch.xpack.rollup.v2.indexer.metrics.MetricField;

import java.io.Closeable;
import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * An indexer for rollup that sorts the buckets from the provided source shard on disk and send them
 * to the target rollup index.
 */
public abstract class RollupShardIndexer {
    private static final Logger logger = LogManager.getLogger(RollupShardIndexer.class);

    protected final IndexShard indexShard;
    private final Client client;
    protected final RollupActionConfig config;
    protected final String tmpIndex;

    protected final Engine.Searcher searcher;
    protected final SearchExecutionContext searchExecutionContext;

    protected final FieldValueFetcher timestampFetcher;
    protected final List<FieldValueFetcher> groupFieldFetchers;
    protected final MetricField[] metricFields;

    protected final Rounding.Prepared rounding;
    protected final BulkProcessor bulkProcessor;
    protected final AtomicLong numReceived = new AtomicLong();
    protected final AtomicLong numSkip = new AtomicLong();
    protected final AtomicLong numSent = new AtomicLong();
    protected final AtomicLong numIndexed = new AtomicLong();
    protected final AtomicLong numFailed = new AtomicLong();

    protected RollupShardStatus status;

    protected RollupShardIndexer(
        RollupShardStatus rollupShardStatus,
        Client client,
        IndexService indexService,
        ShardId shardId,
        RollupActionConfig config,
        String tmpIndex
    ) {
        this.status = rollupShardStatus;
        status.init(numReceived, numSkip, numSent, numIndexed, numFailed);
        this.client = client;
        this.indexShard = indexService.getShard(shardId.id());
        this.config = config;
        this.tmpIndex = tmpIndex;

        this.indexShard.refresh("rollup");
        this.searcher = indexShard.acquireSearcher("rollup");
        Closeable toClose = searcher;
        try {
            this.searchExecutionContext = indexService.newSearchExecutionContext(
                indexShard.shardId().id(),
                0,
                searcher,
                () -> 0L,
                null,
                Collections.emptyMap()
            );
            this.timestampFetcher = FieldValueFetcher.build(searchExecutionContext, config.getGroupConfig().getDateHistogram().getField());
            verifyTimestampField(this.timestampFetcher.fieldType);
            this.rounding = createRounding(config.getGroupConfig().getDateHistogram()).prepareForUnknown();
            this.groupFieldFetchers = new ArrayList<>();

            if (config.getGroupConfig().getTerms() != null) {
                TermsGroupConfig termsConfig = config.getGroupConfig().getTerms();
                this.groupFieldFetchers.addAll(FieldValueFetcher.buildList(searchExecutionContext, termsConfig.getFields()));
            }

            if (config.getGroupConfig().getHistogram() != null) {
                HistogramGroupConfig histoConfig = config.getGroupConfig().getHistogram();
                this.groupFieldFetchers.addAll(
                    FieldValueFetcher.buildHistograms(searchExecutionContext, histoConfig.getFields(), histoConfig.getInterval())
                );
            }

            if (config.getMetricsConfig().size() > 0) {
                List<MetricField> metricFieldList = new ArrayList<>();
                for (MetricConfig metricConfig : config.getMetricsConfig()) {
                    FieldValueFetcher fetcher = FieldValueFetcher.build(searchExecutionContext, metricConfig.getField());
                    metricFieldList.add(MetricField.buildMetricField(metricConfig, fetcher));
                }
                this.metricFields = metricFieldList.toArray(new MetricField[0]);
            } else {
                this.metricFields = new MetricField[0];
            }

            toClose = null;
        } finally {
            IOUtils.closeWhileHandlingException(toClose);
        }

        this.bulkProcessor = createBulkProcessor();

        status.setStatus(Status.ROLLING);
    }

    public abstract void execute() throws IOException;

    private void verifyTimestampField(MappedFieldType fieldType) {
        if (fieldType == null) {
            throw new IllegalArgumentException("fieldType is null");
        }
        if (fieldType instanceof DateFieldMapper.DateFieldType == false) {
            throw new IllegalArgumentException("Wrong type for the timestamp field, " + "expected [date], got [" + fieldType.name() + "]");
        }
        if (fieldType.isIndexed() == false) {
            throw new IllegalArgumentException("The timestamp field [" + fieldType.name() + "]  is not indexed");
        }
    }

    protected boolean isCanceled() {
        if (status.getStatus() == Status.ABORT || status.getStatus() == Status.STOP) {
            return true;
        } else {
            return false;
        }
    }

    private BulkProcessor createBulkProcessor() {
        final BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                numSent.addAndGet(request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                numIndexed.addAndGet(request.numberOfActions());
                if (response.hasFailures()) {
                    Map<String, String> failures = Arrays.stream(response.getItems())
                        .filter(BulkItemResponse::isFailed)
                        .collect(
                            Collectors.toMap(
                                BulkItemResponse::getId,
                                BulkItemResponse::getFailureMessage,
                                (msg1, msg2) -> Objects.equals(msg1, msg2) ? msg1 : msg1 + "," + msg2
                            )
                        );
                    numFailed.addAndGet(failures.size());
                    logger.debug(
                        "[{}] rollup index failures summery: failed count: [{}], failed sample: [{}]",
                        indexShard.shardId(),
                        failures.size(),
                        failures.values().iterator().next()
                    );
                    logger.trace("[{}] rollup index failures: [{}]", indexShard.shardId(), failures);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                long items = request.numberOfActions();
                numSent.addAndGet(-items);
                numFailed.addAndGet(items);
                logger.debug(() -> new ParameterizedMessage("[{}] rollup index bulk failed", indexShard.shardId()), failure);
            }
        };
        return BulkProcessor.builder(client::bulk, listener, "rollup-shard-indexer")
            .setBulkActions(10000)
            .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.MB))
            // execute the bulk request on the same thread
            .setConcurrentRequests(0)
            .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(1000), 3))
            .build();
    }

    protected Rounding createRounding(RollupActionDateHistogramGroupConfig groupConfig) {
        DateHistogramInterval interval = groupConfig.getInterval();
        ZoneId zoneId = groupConfig.getTimeZone() != null ? ZoneId.of(groupConfig.getTimeZone()) : null;
        Rounding.Builder tzRoundingBuilder;
        if (groupConfig instanceof RollupActionDateHistogramGroupConfig.FixedInterval) {
            TimeValue timeValue = TimeValue.parseTimeValue(interval.toString(), null, getClass().getSimpleName() + ".interval");
            tzRoundingBuilder = Rounding.builder(timeValue);
        } else if (groupConfig instanceof RollupActionDateHistogramGroupConfig.CalendarInterval) {
            Rounding.DateTimeUnit dateTimeUnit = DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(interval.toString());
            tzRoundingBuilder = Rounding.builder(dateTimeUnit);
        } else {
            throw new IllegalStateException("unsupported interval type");
        }
        return tzRoundingBuilder.timeZone(zoneId).build();
    }

    protected void indexBucket(BucketKey key, int docCount) throws IOException {
        IndexRequestBuilder request = client.prepareIndex(tmpIndex);
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.SMILE);
        builder.startObject();
        builder.field(DocCountFieldMapper.NAME, docCount);
        builder.field(timestampFetcher.fieldType.name(), timestampFetcher.format.format(key.timestamp));

        Set<String> groups = new HashSet<>();
        for (int i = 0; i < key.groupFields.size(); i++) {
            FieldValueFetcher fetcher = groupFieldFetchers.get(i);
            if (key.groupFields.get(i) != null) {
                if (fetcher.format instanceof TimeSeriesIdDocValueFormat) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> dimensionFields = (Map<String, Object>) key.groupFields.get(i);
                    for (Map.Entry<String, Object> entry : dimensionFields.entrySet()) {
                        if (groups.contains(entry.getKey())) {
                            continue;
                        }
                        builder.field(entry.getKey(), entry.getValue());
                        groups.add(entry.getKey());
                    }
                } else if (false == groups.contains(fetcher.name)) {
                    builder.field(fetcher.name, fetcher.format(key.groupFields.get(i)));
                }
            }
        }

        for (MetricField metricField : metricFields) {
            Map<String, Object> map = null;
            boolean metricMissing = false;
            for (MetricCollector metric : metricField.getCollectors()) {
                if (metric.get() == null) {
                    metricMissing = true;
                    break;
                }
                if (map == null) {
                    map = new HashMap<>();
                }
                map.put(metric.name, metric.get());
            }

            if (false == metricMissing) {
                builder.field(metricField.getName(), map);
            }
        }

        builder.endObject();
        request.setSource(builder);
        bulkProcessor.add(request.request());
    }

    protected static class BucketKey {
        final long timestamp;
        final List<Object> groupFields;

        public BucketKey(long timestamp, List<Object> groupFields) {
            this.timestamp = timestamp;
            this.groupFields = groupFields;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public List<Object> getGroupFields() {
            return groupFields;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BucketKey other = (BucketKey) o;
            return timestamp == other.timestamp && Objects.equals(groupFields, other.groupFields);
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, groupFields);
        }

        @Override
        public String toString() {
            return "BucketKey{" + "timestamp=" + timestamp + ", groupFields=" + groupFields + '}';
        }
    }

    protected FormattedDocValues[] leafGroupFetchers(LeafReaderContext context) {
        List<FormattedDocValues> leaves = new ArrayList<>();
        for (FieldValueFetcher fetcher : groupFieldFetchers) {
            leaves.add(fetcher.getGroupLeaf(context));
        }
        return leaves.toArray(new FormattedDocValues[0]);
    }

    protected LeafMetricField[] leafMetricFields(LeafReaderContext context) {
        List<LeafMetricField> leaves = new ArrayList<>();
        for (MetricField metricField : metricFields) {
            leaves.add(metricField.getMetricFieldLeaf(context));
        }
        return leaves.toArray(new LeafMetricField[0]);
    }

    protected void resetMetricCollectors() {
        for (MetricField metricField : metricFields) {
            metricField.resetCollectors();
        }
    }
}
