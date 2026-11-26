/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.timeseries;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.codec.tsdb.PartitionedDocValues;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Collects the {@link TsidPrefixHistogram} of the indices on given clusters.
 */
public final class TsidPrefixHistogramCollector {

    private TsidPrefixHistogramCollector() {}

    /**
     * Collects a {@link TsidPrefixHistogram} for the given indices.
     *
     * @param concreteIndices resolved, authorized concrete index names by cluster
     * @param originalIndices original index expressions by cluster, used for authorization
     * @param metricFields    metrics to match when selecting prefix cells; at least one metric must be specified
     */
    public static void collect(
        TransportService transportService,
        CancellableTask queryTask,
        Map<String, String[]> concreteIndices,
        Map<String, OriginalIndices> originalIndices,
        List<String> metricFields,
        long minTimestamp,
        long maxTimestamp,
        long bucketMillis,
        ActionListener<TsidPrefixHistogramResponse> listener
    ) {
        if (metricFields.isEmpty()) {
            listener.onFailure(new IllegalArgumentException("collecting a tsid prefix histogram requires metric fields"));
            return;
        }
        if (queryTask.notifyIfCancelled(listener)) {
            return;
        }
        var clusters = concreteIndices.entrySet().stream().filter(e -> e.getValue().length > 0).toList();
        if (clusters.isEmpty()) {
            listener.onResponse(TsidPrefixHistogramResponse.mergeResponses(List.of()));
            return;
        }
        var responses = new GroupedActionListener<>(clusters.size(), listener.map(TsidPrefixHistogramResponse::mergeResponses));
        for (var entry : clusters) {
            String clusterAlias = entry.getKey();
            ActionListener<TsidPrefixHistogramResponse> clusterListener = responses.delegateResponse((delegate, e) -> {
                if (ExceptionsHelper.isTaskCancelledException(e)) {
                    delegate.onFailure(e);
                } else if (queryTask.notifyIfCancelled(delegate) == false) {
                    delegate.onResponse(TsidPrefixHistogramResponse.clusterFailure(clusterAlias, e));
                }
            });
            OriginalIndices indices = originalIndices.get(clusterAlias);
            if (indices == null) {
                String cluster = Strings.isEmpty(clusterAlias) ? "local" : clusterAlias;
                clusterListener.onFailure(new IllegalStateException("missing original index expressions for cluster [" + cluster + "]"));
                continue;
            }
            var request = new TsidPrefixHistogramRequest(indices, entry.getValue(), metricFields, minTimestamp, maxTimestamp, bucketMillis);
            collectOneCluster(transportService, queryTask, clusterAlias, request, clusterListener);
        }
    }

    private static void collectOneCluster(
        TransportService transportService,
        CancellableTask queryTask,
        String clusterAlias,
        TsidPrefixHistogramRequest request,
        ActionListener<TsidPrefixHistogramResponse> listener
    ) {
        final Transport.Connection connection;
        try {
            connection = Strings.isEmpty(clusterAlias)
                ? transportService.getConnection(transportService.getLocalNode())
                : transportService.getRemoteClusterService().getConnection(clusterAlias);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        transportService.sendChildRequest(
            connection,
            TransportTsidPrefixHistogramAction.TYPE.name(),
            request,
            queryTask,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(
                listener,
                TsidPrefixHistogramResponse::new,
                transportService.getThreadPool().executor(ThreadPool.Names.SEARCH_COORDINATION)
            )
        );
    }

    static TsidPrefixHistogram collectShard(
        IndexService indexService,
        ShardId shardId,
        TsidPrefixHistogramNodeRequest request,
        CancellableTask task
    ) throws IOException {
        var timestampType = (DateFieldMapper.DateFieldType) indexService.mapperService().fieldType(DataStream.TIMESTAMP_FIELD_NAME);
        TsidPrefixHistogram mergedHistogram = TsidPrefixHistogram.EMPTY;
        // Use shard-wide planning statistics independent of DLS and alias filters.
        // Field visibility follows the wrapped searcher.
        try (Engine.Searcher searcher = indexService.getShard(shardId.id()).acquireSearcher("esql_tsid_prefix_histogram")) {
            SearchExecutionContext context = indexService.newSearchExecutionContext(
                shardId.id(),
                0,
                searcher,
                System::currentTimeMillis,
                null,
                Map.of(),
                null,
                null
            );
            Query filter = metricFilter(request.metricFields, context);
            Weight weight = searcher.createWeight(searcher.rewrite(filter), ScoreMode.COMPLETE_NO_SCORES, 1f);
            PartitionedDocValues.PrefixPartitions partitions = null;
            for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
                task.ensureNotCancelled();
                SortedDocValues tsid = leaf.reader().getSortedDocValues(TimeSeriesIdFieldMapper.NAME);
                if (tsid == null || tsid.getValueCount() == 0) {
                    continue;
                }
                long[] timestamps = timestampRange(leaf.reader(), timestampType);
                long minTimestamp = Math.max(timestamps[0], request.minTimestamp);
                long maxTimestamp = Math.min(timestamps[1], request.maxTimestamp);
                if (minTimestamp > maxTimestamp) {
                    continue;
                }
                if (tsid instanceof PartitionedDocValues == false) {
                    throw new IllegalStateException("shard " + shardId + " has a segment without tsid prefix");
                }
                Scorer scorer = weight.scorer(leaf);
                if (scorer == null) {
                    continue;
                }
                PartitionedDocValues partitioned = (PartitionedDocValues) tsid;
                partitions = partitioned.prefixPartitions(partitions);
                if (partitions == null) {
                    throw new IllegalStateException("shard " + shardId + " has a segment without tsid prefix");
                }
                TsidPrefixHistogram histogram = collectOneLeaf(
                    partitions,
                    partitioned.prefixPartitionBits(),
                    leaf.reader().maxDoc(),
                    scorer.iterator(),
                    tsid,
                    minTimestamp,
                    maxTimestamp,
                    request.bucketMillis
                );
                mergedHistogram = TsidPrefixHistogram.merge(mergedHistogram, histogram);
            }
        }
        return mergedHistogram;
    }

    static TsidPrefixHistogram collectOneLeaf(
        PartitionedDocValues.PrefixPartitions partitions,
        int prefixBits,
        int maxDoc,
        DocIdSetIterator matching,
        SortedDocValues tsid,
        long minTimestamp,
        long maxTimestamp,
        long bucketMillis
    ) throws IOException {
        assert minTimestamp <= maxTimestamp;
        final double spanMillis = TsidPrefixHistogram.timeSpanMillis(minTimestamp, maxTimestamp);
        final long docs = matching.cost();
        final int numPartitions = partitions.numPartitions();
        final int[] startDocs = partitions.startDocs();
        final int[] prefixes = new int[numPartitions];
        final double[] seriesMillis = new double[numPartitions];
        int size = 0;
        int lastDoc = -1;
        int lastOrd = -1;
        int doc = -1;
        for (int partition = 0; partition < numPartitions; partition++) {
            final int startDoc = startDocs[partition];
            final int endDoc = startDoc(partitions, partition + 1, maxDoc);
            if (doc < startDoc) {
                doc = matching.advance(startDoc);
            }
            if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                break;
            }
            if (doc >= endDoc) {
                continue;
            }
            // the next partition starts where this one ends, so walking partitions in order looks up each ordinal once
            final int startOrd = startDoc == lastDoc ? lastOrd : tsidOrd(tsid, startDoc, maxDoc);
            final int endOrd = tsidOrd(tsid, endDoc, maxDoc);
            lastDoc = endDoc;
            lastOrd = endOrd;
            prefixes[size] = partitions.prefixes()[partition];
            seriesMillis[size++] = (endOrd - startOrd) * spanMillis;
        }
        if (bucketMillis > 0) {
            double totalSeriesMillis = 0;
            for (int i = 0; i < size; i++) {
                totalSeriesMillis += seriesMillis[i];
            }
            final double scale = (double) docs * bucketMillis / totalSeriesMillis;
            if (scale < 1) {
                for (int i = 0; i < size; i++) {
                    seriesMillis[i] = Math.ceil(seriesMillis[i] * scale);
                }
            }
        }
        return new TsidPrefixHistogram(prefixBits, prefixes, seriesMillis, size, minTimestamp, maxTimestamp, docs);
    }

    private static int startDoc(PartitionedDocValues.PrefixPartitions partitions, int partition, int maxDoc) {
        return partition < partitions.numPartitions() ? partitions.startDocs()[partition] : maxDoc;
    }

    private static int tsidOrd(SortedDocValues tsid, int doc, int maxDoc) throws IOException {
        if (doc >= maxDoc) {
            return tsid.getValueCount();
        }
        final boolean found = tsid.advanceExact(doc);
        assert found : "every time-series doc has a _tsid";
        return tsid.ordValue();
    }

    static Query metricFilter(List<String> metricFields, SearchExecutionContext context) {
        BooleanQuery.Builder exists = new BooleanQuery.Builder();
        for (String field : metricFields) {
            if (context.isFieldMapped(field)) {
                exists.add(context.getFieldType(field).existsQuery(context), BooleanClause.Occur.SHOULD);
            }
        }
        return exists.build();
    }

    private static long[] timestampRange(LeafReader reader, DateFieldMapper.DateFieldType timestamp) throws IOException {
        long min;
        long max;
        DocValuesSkipper skipper = reader.getDocValuesSkipper(DataStream.TIMESTAMP_FIELD_NAME);
        if (skipper != null) {
            min = skipper.minValue();
            max = skipper.maxValue();
        } else {
            PointValues points = reader.getPointValues(DataStream.TIMESTAMP_FIELD_NAME);
            if (points == null) {
                throw new IllegalStateException("time-series segment without " + DataStream.TIMESTAMP_FIELD_NAME);
            }
            min = LongPoint.decodeDimension(points.getMinPackedValue(), 0);
            max = LongPoint.decodeDimension(points.getMaxPackedValue(), 0);
        }
        DateFieldMapper.Resolution resolution = timestamp.resolution();
        return new long[] { resolution.roundDownToMillis(min), resolution.roundUpToMillis(max) };
    }
}
