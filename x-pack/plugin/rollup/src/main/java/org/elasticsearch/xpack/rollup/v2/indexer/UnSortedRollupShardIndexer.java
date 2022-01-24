/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2.indexer;

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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.CancellableThreads.ExecutionCancelledException;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.aggregations.bucket.DocCountProvider;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.action.RollupShardStatus;
import org.elasticsearch.xpack.core.rollup.action.RollupShardStatus.Status;
import org.elasticsearch.xpack.rollup.v2.RollupShardIndexer;
import org.elasticsearch.xpack.rollup.v2.indexer.metrics.LeafMetricField;
import org.elasticsearch.xpack.rollup.v2.indexer.metrics.MetricField;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * rollup data in unsorted mode
 */
public class UnSortedRollupShardIndexer extends RollupShardIndexer {
    private static final Logger logger = LogManager.getLogger(UnSortedRollupShardIndexer.class);

    private final Directory dir;
    private final CompressingOfflineSorter sorter;

    // for testing
    public final Set<String> tmpFiles = new HashSet<>();
    public final Set<String> tmpFilesDeleted = new HashSet<>();

    public UnSortedRollupShardIndexer(
        RollupShardStatus rollupShardStatus,
        Client client,
        IndexService indexService,
        ShardId shardId,
        RollupActionConfig config,
        String tmpIndex,
        int ramBufferSizeMB
    ) {
        super(rollupShardStatus, client, indexService, shardId, config, tmpIndex);

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
            this.sorter = new CompressingOfflineSorter(dir, "rollup-", keyComparator(), ramBufferSizeMB);
            toClose = null;
        } finally {
            IOUtils.closeWhileHandlingException(toClose);
        }
    }

    @Override
    public void execute() throws IOException {
        Long bucket = Long.MIN_VALUE;
        long count = 0;
        long start = System.currentTimeMillis();
        try (searcher; bulkProcessor) {
            do {
                if (isCanceled()) {
                    logger.warn(
                        "[{}] rolling abort, sent [{}], indexed [{}], failed[{}]",
                        indexShard.shardId(),
                        numIndexed.get(),
                        numIndexed.get(),
                        numFailed.get()
                    );
                    throw new ExecutionCancelledException("[" + indexShard.shardId() + "] rollup cancelled");
                }

                long startCompute = System.currentTimeMillis();
                bucket = computeBucket(bucket);
                logger.debug(
                    "[{}] rollup, current computeBucket cost:{}, bucket:{}, loop:{}",
                    indexShard.shardId(),
                    (System.currentTimeMillis() - startCompute),
                    bucket,
                    ++count
                );
            } while (bucket != null);

            bulkProcessor.flush();
        }

        logger.info(
            "sorted rollup execute [{}], cost [{}], Received [{}], Skip [{}], Successfully sent [{}], indexed [{}], failed[{}]",
            indexShard.shardId(),
            (System.currentTimeMillis() - start),
            numReceived.get(),
            numSkip.get(),
            numSent.get(),
            numIndexed.get(),
            numFailed.get()
        );
        status.setStatus(Status.STOP);
        return;
    }

    private Long computeBucket(long lastRounding) throws IOException {
        Long nextRounding = findNextRounding(lastRounding);
        if (nextRounding == null) {
            return null;
        }
        long nextRoundingLastValue = rounding.nextRoundingValue(nextRounding) - 1;
        try (XExternalRefSorter externalSorter = new XExternalRefSorter(sorter)) {
            long start = System.currentTimeMillis();
            Query rangeQuery = LongPoint.newRangeQuery(timestampFetcher.getFieldType().name(), nextRounding, nextRoundingLastValue);
            searcher.search(rangeQuery, new BucketCollector(nextRounding, externalSorter));
            long searchTime = System.currentTimeMillis();
            logger.debug("current round [{}], search cost [{}]", nextRounding, (searchTime - start));

            BytesRefIterator it = externalSorter.iterator();
            BytesRef next = it.next();

            BucketKey lastKey = null;
            int docCount = 0;
            int keyCount = 0;
            int totalDocCount = 0;
            while (next != null) {
                try (StreamInput in = new ByteBufferStreamInput(ByteBuffer.wrap(next.bytes, next.offset, next.length))) {
                    // skip key size
                    in.readInt();
                    BucketKey key = decodeKey(in);
                    totalDocCount++;
                    if (lastKey != null && lastKey.equals(key) == false) {
                        indexBucket(lastKey, docCount);
                        keyCount++;
                        docCount = 0;
                        resetMetricCollectors();
                    }
                    for (MetricField metricField : metricFields) {
                        metricField.collectMetric(in);
                    }
                    docCount += in.readVInt();
                    lastKey = key;
                }
                next = it.next();
            }
            if (lastKey != null) {
                indexBucket(lastKey, docCount);
                keyCount++;
                resetMetricCollectors();
            }

            logger.debug(
                "current round [{}], doc count [{}], keyCount [{}], build index data cost [{}]",
                nextRounding,
                totalDocCount,
                keyCount,
                (System.currentTimeMillis() - searchTime)
            );
        }
        return nextRoundingLastValue;
    }

    private Long findNextRounding(long lastRounding) throws IOException {
        Long nextRounding = null;
        for (LeafReaderContext leafReaderContext : searcher.getIndexReader().leaves()) {
            PointValues pointValues = leafReaderContext.reader().getPointValues(timestampFetcher.getFieldType().name());
            if (pointValues == null) {
                continue;
            }
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
            out.writeVInt(groupFields.size());
            for (Object obj : groupFields) {
                out.writeGenericValue(obj);
            }
            return out.bytes().toBytesRef();
        }
    }

    private static BucketKey decodeKey(StreamInput in) throws IOException {
        long timestamp = in.readLong();
        int numGroupFields = in.readVInt();
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
            return Arrays.compareUnsigned(
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

    private class BucketCollector implements Collector {
        private final long timestamp;
        private final XExternalRefSorter externalSorter;

        private BucketCollector(long timestamp, XExternalRefSorter externalSorter) {
            this.externalSorter = externalSorter;
            this.timestamp = timestamp;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) {
            final FormattedDocValues[] leafGroupFetchers = leafGroupFetchers(context);
            final LeafMetricField[] leafMetricFields = leafMetricFields(context);
            final DocCountProvider docCountProvider = new DocCountProvider();
            return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {}

                @Override
                public void collect(int docID) throws IOException {
                    if (isCanceled()) {
                        // TODO check if this can throw cancel exception
                        return;
                    }

                    numReceived.incrementAndGet();
                    docCountProvider.setLeafReaderContext(context);
                    List<List<Object>> combinationKeys = new ArrayList<>();
                    for (FormattedDocValues leafField : leafGroupFetchers) {
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
                        for (LeafMetricField leafMetricField : leafMetricFields) {
                            leafMetricField.writeMetrics(docID, out);
                        }
                        valueBytes = out.bytes().toBytesRef();
                    }
                    for (List<Object> groupFields : cartesianProduct(combinationKeys)) {
                        try (BytesStreamOutput out = new BytesStreamOutput()) {
                            BytesRef keyBytes = encodeKey(timestamp, groupFields);
                            out.writeInt(keyBytes.length);
                            out.writeBytes(keyBytes.bytes, keyBytes.offset, keyBytes.length);
                            out.writeBytes(valueBytes.bytes, valueBytes.offset, valueBytes.length);
                            out.writeVInt(docCountProvider.getDocCount(docID));
                            externalSorter.add(out.bytes().toBytesRef());
                        }
                    }
                }
            };
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

        private void checkMinRounding(long roundingValue) {
            if (roundingValue > lastRounding) {
                nextRounding = roundingValue;
                throw new CollectionTerminatedException();
            }
        }
    }

    private static List<List<Object>> cartesianProduct(List<List<Object>> lists) {
        List<List<Object>> combinations = Arrays.asList(Arrays.asList());
        for (List<Object> list : lists) {
            if (list == null) {
                continue;
            }
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
