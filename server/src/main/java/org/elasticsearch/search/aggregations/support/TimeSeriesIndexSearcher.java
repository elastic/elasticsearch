/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntSupplier;

import static org.elasticsearch.index.IndexSortConfig.TIME_SERIES_SORT;

/**
 * An IndexSearcher wrapper that executes the searches in time-series indices by traversing them by tsid and timestamp
 * TODO: Convert it to use index sort instead of hard-coded tsid and timestamp values
 */
public class TimeSeriesIndexSearcher {
    private static final int CHECK_CANCELLED_SCORER_INTERVAL = 1 << 11;

    // We need to delegate to the other searcher here as opposed to extending IndexSearcher and inheriting default implementations as the
    // IndexSearcher would most of the time be a ContextIndexSearcher that has important logic related to e.g. document-level security.
    private final ContextIndexSearcher searcher;
    private final List<Runnable> cancellations;
    private final boolean tsidReverse;
    private final boolean timestampReverse;

    public TimeSeriesIndexSearcher(IndexSearcher searcher, List<Runnable> cancellations) {
        try {
            this.searcher = new ContextIndexSearcher(
                searcher.getIndexReader(),
                searcher.getSimilarity(),
                searcher.getQueryCache(),
                searcher.getQueryCachingPolicy(),
                false
            );
        } catch (IOException e) {
            // IOException from wrapping the index searcher which should never happen.
            throw new RuntimeException(e);
        }
        this.cancellations = cancellations;
        cancellations.forEach(cancellation -> this.searcher.addQueryCancellation(cancellation));

        assert TIME_SERIES_SORT.length == 2;
        assert TIME_SERIES_SORT[0].getField().equals(TimeSeriesIdFieldMapper.NAME);
        assert TIME_SERIES_SORT[1].getField().equals(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        this.tsidReverse = TIME_SERIES_SORT[0].getOrder() == SortOrder.DESC;
        this.timestampReverse = TIME_SERIES_SORT[1].getOrder() == SortOrder.DESC;
    }

    public void search(Query query, BucketCollector bucketCollector) throws IOException {
        int seen = 0;
        query = searcher.rewrite(query);
        Weight weight = searcher.createWeight(query, bucketCollector.scoreMode(), 1);
        int[] tsidOrd = new int[1];

        // Create LeafWalker for each subreader
        List<LeafWalker> leafWalkers = new ArrayList<>();
        for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
            if (++seen % CHECK_CANCELLED_SCORER_INTERVAL == 0) {
                checkCancelled();
            }
            Scorer scorer = weight.scorer(leaf);
            if (scorer != null) {
                LeafWalker leafWalker = new LeafWalker(leaf, scorer, bucketCollector, () -> tsidOrd[0]);
                if (leafWalker.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    leafWalkers.add(leafWalker);
                }
            } else {
                // Even though we will not walk through this aggregation as a part of normal processing
                // this is needed to trigger actions in some bucketCollectors that bypass the normal iteration logic
                // for example, global aggregator triggers a separate iterator that ignores the query but still needs
                // to know all leaves
                bucketCollector.getLeafCollector(new AggregationExecutionContext(leaf, null, null, null));
            }
        }

        PriorityQueue<LeafWalker> queue = new PriorityQueue<>(searcher.getIndexReader().leaves().size()) {
            @Override
            protected boolean lessThan(LeafWalker a, LeafWalker b) {
                if (timestampReverse) {
                    return a.timestamp > b.timestamp;
                } else {
                    return a.timestamp < b.timestamp;
                }
            }
        };

        // The priority queue is filled for each TSID in order. When a walker moves
        // to the next TSID it is removed from the queue. Once the queue is empty,
        // we refill it with walkers positioned on the next TSID. Within the queue
        // walkers are ordered by timestamp.
        while (populateQueue(leafWalkers, queue)) {
            do {
                if (++seen % CHECK_CANCELLED_SCORER_INTERVAL == 0) {
                    checkCancelled();
                }
                LeafWalker walker = queue.top();
                walker.collectCurrent();
                if (walker.nextDoc() == DocIdSetIterator.NO_MORE_DOCS || walker.shouldPop()) {
                    queue.pop();
                } else {
                    queue.updateTop();
                }
            } while (queue.size() > 0);
            tsidOrd[0]++;
        }
    }

    public void setProfiler(SearchContext context) {
        if ((context.getProfilers() != null) && (context.getProfilers().getCurrentQueryProfiler() != null)) {
            searcher.setProfiler(context.getProfilers().getCurrentQueryProfiler());
        }
    }

    // Re-populate the queue with walkers on the same TSID.
    private boolean populateQueue(List<LeafWalker> leafWalkers, PriorityQueue<LeafWalker> queue) throws IOException {
        BytesRef currentTsid = null;
        assert queue.size() == 0;
        Iterator<LeafWalker> it = leafWalkers.iterator();
        while (it.hasNext()) {
            LeafWalker leafWalker = it.next();
            if (leafWalker.docId == DocIdSetIterator.NO_MORE_DOCS) {
                // If a walker is exhausted then we can remove it from consideration
                // entirely
                it.remove();
                continue;
            }
            BytesRef tsid = leafWalker.getTsid();
            if (currentTsid == null) {
                currentTsid = tsid;
            }
            int comp = tsid.compareTo(currentTsid);
            if (comp == 0) {
                queue.add(leafWalker);
            } else if ((tsidReverse && comp > 0) || (false == tsidReverse && comp < 0)) {
                // We've found a walker on a lower TSID, so we remove all walkers
                // collected so far from the queue and reset our comparison TSID
                // to be the lower value
                queue.clear();
                queue.add(leafWalker);
                currentTsid = tsid;
            }
        }
        assert queueAllHaveTsid(queue, currentTsid);
        // If all walkers are exhausted then nothing will have been added to the queue
        // and we're done
        return queue.size() > 0;
    }

    private static boolean queueAllHaveTsid(PriorityQueue<LeafWalker> queue, BytesRef tsid) throws IOException {
        for (LeafWalker leafWalker : queue) {
            BytesRef walkerId = leafWalker.tsids.lookupOrd(leafWalker.tsids.ordValue());
            assert walkerId.equals(tsid) : tsid.utf8ToString() + " != " + walkerId.utf8ToString();
        }
        return true;
    }

    private void checkCancelled() {
        for (Runnable r : cancellations) {
            r.run();
        }
    }

    private static class LeafWalker {
        private final LeafBucketCollector collector;
        private final Bits liveDocs;
        private final DocIdSetIterator iterator;
        private final SortedDocValues tsids;
        private final SortedNumericDocValues timestamps;    // TODO can we have this just a NumericDocValues?
        private final BytesRefBuilder scratch = new BytesRefBuilder();
        int docId = -1;
        int tsidOrd;
        long timestamp;

        LeafWalker(LeafReaderContext context, Scorer scorer, BucketCollector bucketCollector, IntSupplier tsidOrdSupplier)
            throws IOException {
            AggregationExecutionContext aggCtx = new AggregationExecutionContext(context, scratch::get, () -> timestamp, tsidOrdSupplier);
            this.collector = bucketCollector.getLeafCollector(aggCtx);
            liveDocs = context.reader().getLiveDocs();
            this.collector.setScorer(scorer);
            iterator = scorer.iterator();
            tsids = DocValues.getSorted(context.reader(), TimeSeriesIdFieldMapper.NAME);
            timestamps = DocValues.getSortedNumeric(context.reader(), DataStream.TimestampField.FIXED_TIMESTAMP_FIELD);
        }

        void collectCurrent() throws IOException {
            assert tsids.docID() == docId;
            assert timestamps.docID() == docId;
            collector.collect(docId);
        }

        int nextDoc() throws IOException {
            if (docId == DocIdSetIterator.NO_MORE_DOCS) {
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            do {
                docId = iterator.nextDoc();
            } while (docId != DocIdSetIterator.NO_MORE_DOCS && isInvalidDoc(docId));
            if (docId != DocIdSetIterator.NO_MORE_DOCS) {
                timestamp = timestamps.nextValue();
            }
            return docId;
        }

        BytesRef getTsid() throws IOException {
            tsidOrd = tsids.ordValue();
            scratch.copyBytes(tsids.lookupOrd(tsidOrd));
            return scratch.get();
        }

        // invalid if the doc is deleted or if it doesn't have a tsid or timestamp entry
        private boolean isInvalidDoc(int docId) throws IOException {
            return (liveDocs != null && liveDocs.get(docId) == false)
                || tsids.advanceExact(docId) == false
                || timestamps.advanceExact(docId) == false;
        }

        // true if the TSID ord has changed since the last time we checked
        boolean shouldPop() throws IOException {
            if (tsidOrd != tsids.ordValue()) {
                return true;
            } else {
                return false;
            }
        }
    }
}
