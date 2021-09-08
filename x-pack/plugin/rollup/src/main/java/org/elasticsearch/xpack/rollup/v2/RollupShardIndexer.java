/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.v2;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.FutureArrays;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
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
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionDateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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
class RollupShardIndexer {
    private static final Logger logger = LogManager.getLogger(RollupShardIndexer.class);

    private final IndexShard indexShard;
    private final Client client;
    private final RollupActionConfig config;
    private final String tmpIndex;

    private final Directory dir;
    private final Engine.Searcher searcher;
    private final SearchExecutionContext searchExecutionContext;
    private final MappedFieldType timestampField;
    private final DocValueFormat timestampFormat;
    private final Rounding.Prepared rounding;

    private final List<FieldValueFetcher> groupFieldFetchers;
    private final List<FieldValueFetcher> metricsFieldFetchers;

    private final CompressingOfflineSorter sorter;

    private final BulkProcessor bulkProcessor;
    private final AtomicLong numSent = new AtomicLong();
    private final AtomicLong numIndexed = new AtomicLong();

    // for testing
    final Set<String> tmpFiles = new HashSet<>();
    final Set<String> tmpFilesDeleted = new HashSet<>();

    RollupShardIndexer(
        Client client,
        IndexService indexService,
        ShardId shardId,
        RollupActionConfig config,
        String tmpIndex,
        int ramBufferSizeMB
    ) {
        this.client = client;
        this.indexShard = indexService.getShard(shardId.id());
        this.config = config;
        this.tmpIndex = tmpIndex;

        this.searcher = indexShard.acquireSearcher("rollup");
        Closeable toClose = searcher;
        try {
            this.dir = new FilterDirectory(searcher.getDirectoryReader().directory()) {
                @Override
                public IndexOutput createOutput(String name, IOContext context) throws IOException {
                    tmpFiles.add(name);
                    return super.createOutput(name, context);
                }

                @Override
                public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
                    IndexOutput output = super.createTempOutput(prefix, suffix, context);
                    tmpFiles.add(output.getName());
                    return output;
                }

                @Override
                public void deleteFile(String name) throws IOException {
                    tmpFilesDeleted.add(name);
                    super.deleteFile(name);
                }
            };
            this.searchExecutionContext = indexService.newSearchExecutionContext(
                indexShard.shardId().id(),
                0,
                searcher,
                () -> 0L,
                null,
                Collections.emptyMap()
            );
            this.timestampField = searchExecutionContext.getFieldType(config.getGroupConfig().getDateHistogram().getField());
            verifyTimestampField(timestampField);
            this.timestampFormat = timestampField.docValueFormat(null, null);
            this.rounding = createRounding(config.getGroupConfig().getDateHistogram()).prepareForUnknown();
            this.groupFieldFetchers = new ArrayList<>();

            if (config.getGroupConfig().getTerms() != null) {
                TermsGroupConfig termsConfig = config.getGroupConfig().getTerms();
                this.groupFieldFetchers.addAll(FieldValueFetcher.build(searchExecutionContext, termsConfig.getFields()));
            }

            if (config.getGroupConfig().getHistogram() != null) {
                HistogramGroupConfig histoConfig = config.getGroupConfig().getHistogram();
                this.groupFieldFetchers.addAll(
                    FieldValueFetcher.buildHistograms(searchExecutionContext, histoConfig.getFields(), histoConfig.getInterval())
                );
            }

            if (config.getMetricsConfig().size() > 0) {
                final String[] metricFields = config.getMetricsConfig().stream().map(MetricConfig::getField).toArray(String[]::new);
                this.metricsFieldFetchers = FieldValueFetcher.build(searchExecutionContext, metricFields);
            } else {
                this.metricsFieldFetchers = Collections.emptyList();
            }

            this.sorter = new CompressingOfflineSorter(dir, "rollup-", keyComparator(), ramBufferSizeMB);
            toClose = null;
        } finally {
            IOUtils.closeWhileHandlingException(toClose);
        }

        this.bulkProcessor = createBulkProcessor();
    }

    private void verifyTimestampField(MappedFieldType fieldType) {
        if (fieldType == null) {
            throw new IllegalArgumentException("fieldType is null");
        }
        if (fieldType instanceof DateFieldMapper.DateFieldType == false) {
            throw new IllegalArgumentException("Wrong type for the timestamp field, " + "expected [date], got [" + fieldType.name() + "]");
        }
        if (fieldType.isSearchable() == false) {
            throw new IllegalArgumentException("The timestamp field [" + fieldType.name() + "]  is not searchable");
        }
    }

    public long execute() throws IOException {
        Long bucket = Long.MIN_VALUE;
        try (searcher; bulkProcessor) {
            do {
                bucket = computeBucket(bucket);
            } while (bucket != null);
        }
        // TODO: check that numIndexed == numSent, otherwise throw an exception
        logger.info("Successfully sent [" + numIndexed.get() + "], indexed [" + numIndexed.get() + "]");
        return numIndexed.get();
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
                    logger.error("failures: [{}]", failures);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                long items = request.numberOfActions();
                numSent.addAndGet(-items);
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

    private Rounding createRounding(RollupActionDateHistogramGroupConfig config) {
        DateHistogramInterval interval = config.getInterval();
        ZoneId zoneId = config.getTimeZone() != null ? ZoneId.of(config.getTimeZone()) : null;
        Rounding.Builder tzRoundingBuilder;
        if (config instanceof RollupActionDateHistogramGroupConfig.FixedInterval) {
            TimeValue timeValue = TimeValue.parseTimeValue(interval.toString(), null, getClass().getSimpleName() + ".interval");
            tzRoundingBuilder = Rounding.builder(timeValue);
        } else if (config instanceof RollupActionDateHistogramGroupConfig.CalendarInterval) {
            Rounding.DateTimeUnit dateTimeUnit = DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(interval.toString());
            tzRoundingBuilder = Rounding.builder(dateTimeUnit);
        } else {
            throw new IllegalStateException("unsupported interval type");
        }
        return tzRoundingBuilder.timeZone(zoneId).build();
    }

    private void indexBucket(BucketKey key, List<FieldMetricsProducer> fieldsMetrics, int docCount) {
        IndexRequestBuilder request = client.prepareIndex(tmpIndex);
        Map<String, Object> doc = new HashMap<>(2 + key.groupFields.size() + fieldsMetrics.size());
        doc.put(DocCountFieldMapper.NAME, docCount);
        doc.put(timestampField.name(), timestampFormat.format(key.timestamp));

        for (int i = 0; i < key.groupFields.size(); i++) {
            FieldValueFetcher fetcher = groupFieldFetchers.get(i);
            if (key.groupFields.get(i) != null) {
                doc.put(fetcher.name, fetcher.format(key.groupFields.get(i)));
            }
        }

        for (FieldMetricsProducer field : fieldsMetrics) {
            Map<String, Object> map = new HashMap<>();
            for (FieldMetricsProducer.Metric metric : field.metrics) {
                map.put(metric.name, metric.get());
            }
            doc.put(field.fieldName, map);
        }
        request.setSource(doc);
        bulkProcessor.add(request.request());
    }

    private Long computeBucket(long lastRounding) throws IOException {
        Long nextRounding = findNextRounding(lastRounding);
        if (nextRounding == null) {
            return null;
        }
        long nextRoundingLastValue = rounding.nextRoundingValue(nextRounding) - 1;
        try (XExternalRefSorter externalSorter = new XExternalRefSorter(sorter)) {
            Query rangeQuery = LongPoint.newRangeQuery(timestampField.name(), nextRounding, nextRoundingLastValue);
            searcher.search(rangeQuery, new BucketCollector(nextRounding, externalSorter));

            BytesRefIterator it = externalSorter.iterator();
            BytesRef next = it.next();

            List<FieldMetricsProducer> fieldsMetrics = FieldMetricsProducer.buildMetrics(config.getMetricsConfig());
            BucketKey lastKey = null;
            int docCount = 0;
            while (next != null) {
                try (StreamInput in = new ByteBufferStreamInput(ByteBuffer.wrap(next.bytes, next.offset, next.length))) {
                    // skip key size
                    in.readInt();
                    BucketKey key = decodeKey(in, groupFieldFetchers.size());
                    if (lastKey != null && lastKey.equals(key) == false) {
                        indexBucket(lastKey, fieldsMetrics, docCount);
                        docCount = 0;
                        for (FieldMetricsProducer producer : fieldsMetrics) {
                            producer.reset();
                        }
                    }
                    for (FieldMetricsProducer field : fieldsMetrics) {
                        int size = in.readVInt();
                        for (int i = 0; i < size; i++) {
                            double value = in.readDouble();
                            for (FieldMetricsProducer.Metric metric : field.metrics) {
                                metric.collect(value);
                            }
                        }
                    }
                    ++docCount;
                    lastKey = key;
                }
                next = it.next();
            }
            if (lastKey != null) {
                indexBucket(lastKey, fieldsMetrics, docCount);
            }
        }
        return nextRoundingLastValue;
    }

    private Long findNextRounding(long lastRounding) throws IOException {
        Long nextRounding = null;
        for (LeafReaderContext leafReaderContext : searcher.getIndexReader().leaves()) {
            PointValues pointValues = leafReaderContext.reader().getPointValues(timestampField.name());
            final NextRoundingVisitor visitor = new NextRoundingVisitor(rounding, lastRounding);
            try {
                pointValues.intersect(visitor);
            } catch (CollectionTerminatedException exc) {}
            if (visitor.nextRounding != null) {
                nextRounding = nextRounding == null ? visitor.nextRounding : Math.min(nextRounding, visitor.nextRounding);
            }
        }
        return nextRounding;
    }

    private static BytesRef encodeKey(long timestamp, List<Object> groupFields) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeLong(timestamp);
            for (Object obj : groupFields) {
                out.writeGenericValue(obj);
            }
            return out.bytes().toBytesRef();
        }
    }

    private static BucketKey decodeKey(StreamInput in, int numGroupFields) throws IOException {
        long timestamp = in.readLong();
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < numGroupFields; i++) {
            values.add(in.readGenericValue());
        }
        return new BucketKey(timestamp, values);
    }

    /**
     * Returns a {@link Comparator} that can be used to sort inputs created by the {@link BucketCollector}.
     * We just want identical buckets to be consecutive for the merge so this comparator doesn't follow the natural
     * order and simply checks for identical binary keys.
     */
    private static Comparator<BytesRef> keyComparator() {
        return (o1, o2) -> {
            int keySize1 = readInt(o1.bytes, o1.offset);
            int keySize2 = readInt(o2.bytes, o2.offset);
            return FutureArrays.compareUnsigned(
                o1.bytes,
                o1.offset + Integer.BYTES,
                keySize1 + o1.offset + Integer.BYTES,
                o2.bytes,
                o2.offset + Integer.BYTES,
                keySize2 + o2.offset + Integer.BYTES
            );
        };
    }

    private static int readInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 24) | ((bytes[offset + 1] & 0xFF) << 16) | ((bytes[offset + 2] & 0xFF) << 8) | (bytes[offset + 3]
            & 0xFF);
    }

    private static class BucketKey {
        private final long timestamp;
        private final List<Object> groupFields;

        BucketKey(long timestamp, List<Object> groupFields) {
            this.timestamp = timestamp;
            this.groupFields = groupFields;
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

    private class BucketCollector implements Collector {
        private final long timestamp;
        private final XExternalRefSorter externalSorter;

        private BucketCollector(long timestamp, XExternalRefSorter externalSorter) {
            this.externalSorter = externalSorter;
            this.timestamp = timestamp;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) {
            final List<FormattedDocValues> groupFieldLeaves = leafFetchers(context, groupFieldFetchers);
            final List<FormattedDocValues> metricsFieldLeaves = leafFetchers(context, metricsFieldFetchers);
            return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {}

                @Override
                public void collect(int docID) throws IOException {
                    List<List<Object>> combinationKeys = new ArrayList<>();
                    for (FormattedDocValues leafField : groupFieldLeaves) {
                        if (leafField.advanceExact(docID)) {
                            List<Object> lst = new ArrayList<>();
                            for (int i = 0; i < leafField.docValueCount(); i++) {
                                lst.add(leafField.nextValue());
                            }
                            combinationKeys.add(lst);
                        } else {
                            combinationKeys.add(null);
                        }
                    }

                    final BytesRef valueBytes;
                    try (BytesStreamOutput out = new BytesStreamOutput()) {
                        for (FormattedDocValues formattedDocValues : metricsFieldLeaves) {
                            if (formattedDocValues.advanceExact(docID)) {
                                out.writeVInt(formattedDocValues.docValueCount());
                                for (int i = 0; i < formattedDocValues.docValueCount(); i++) {
                                    Object obj = formattedDocValues.nextValue();
                                    if (obj instanceof Number == false) {
                                        throw new IllegalArgumentException("Expected [Number], got [" + obj.getClass() + "]");
                                    }
                                    out.writeDouble(((Number) obj).doubleValue());
                                }
                            } else {
                                out.writeVInt(0);
                            }
                        }
                        valueBytes = out.bytes().toBytesRef();
                    }
                    for (List<Object> groupFields : cartesianProduct(combinationKeys)) {
                        try (BytesStreamOutput out = new BytesStreamOutput()) {
                            BytesRef keyBytes = encodeKey(timestamp, groupFields);
                            out.writeInt(keyBytes.length);
                            out.writeBytes(keyBytes.bytes, keyBytes.offset, keyBytes.length);
                            out.writeBytes(valueBytes.bytes, valueBytes.offset, valueBytes.length);
                            externalSorter.add(out.bytes().toBytesRef());
                        }
                    }
                }
            };
        }

        private List<FormattedDocValues> leafFetchers(LeafReaderContext context, List<FieldValueFetcher> fetchers) {
            List<FormattedDocValues> leaves = new ArrayList<>();
            for (FieldValueFetcher fetcher : fetchers) {
                leaves.add(fetcher.getLeaf(context));
            }
            return leaves;
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    }

    private class NextRoundingVisitor implements PointValues.IntersectVisitor {
        final Rounding.Prepared rounding;
        final long lastRounding;

        Long nextRounding = null;

        NextRoundingVisitor(Rounding.Prepared rounding, long lastRounding) {
            this.rounding = rounding;
            this.lastRounding = lastRounding;
        }

        @Override
        public void visit(int docID) {
            throw new IllegalStateException("should never be called");
        }

        @Override
        public void visit(DocIdSetIterator iterator, byte[] packedValue) {
            long bucket = rounding.round(LongPoint.decodeDimension(packedValue, 0));
            checkMinRounding(bucket);
        }

        @Override
        public void visit(int docID, byte[] packedValue) {
            long bucket = rounding.round(LongPoint.decodeDimension(packedValue, 0));
            checkMinRounding(bucket);
        }

        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            long maxRounding = rounding.round(LongPoint.decodeDimension(maxPackedValue, 0));
            if (maxRounding <= lastRounding) {
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            }
            long minRounding = rounding.round(LongPoint.decodeDimension(minPackedValue, 0));
            checkMinRounding(minRounding);
            return PointValues.Relation.CELL_CROSSES_QUERY;
        }

        private void checkMinRounding(long rounding) {
            if (rounding > lastRounding) {
                nextRounding = rounding;
                throw new CollectionTerminatedException();
            }
        }
    }

    private static List<List<Object>> cartesianProduct(List<List<Object>> lists) {
        List<List<Object>> combinations = Arrays.asList(Arrays.asList());
        for (List<Object> list : lists) {
            List<List<Object>> extraColumnCombinations = new ArrayList<>();
            for (List<Object> combination : combinations) {
                for (Object element : list) {
                    List<Object> newCombination = new ArrayList<>(combination);
                    newCombination.add(element);
                    extraColumnCombinations.add(newCombination);
                }
            }
            combinations = extraColumnCombinations;
        }
        return combinations;
    }
}
