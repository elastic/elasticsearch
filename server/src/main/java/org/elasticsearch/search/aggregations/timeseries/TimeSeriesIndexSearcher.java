/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;

import java.io.IOException;

/**
 * An IndexSearcher wrapper that executes the searches in time-series indices by traversing them by tsid and timestamp
 * TODO: Convert it to use index sort instead of hard-coded tsid and timestamp values
 */
public class TimeSeriesIndexSearcher {

    private final IndexSearcher searcher;

    public TimeSeriesIndexSearcher(IndexSearcher searcher) {
        this.searcher = searcher;
    }

    public void search(Query query, Collector queryCollector) throws IOException {
        query = searcher.rewrite(query);
        Weight weight = searcher.createWeight(query, queryCollector.scoreMode(), 1);
        PriorityQueue<LeafWalker> queue = new PriorityQueue<>(searcher.getIndexReader().leaves().size()) {
            @Override
            protected boolean lessThan(LeafWalker a, LeafWalker b) {
                assert a.tsid != null && a.timestamp != null && b.tsid != null && b.timestamp != null;
                int res = a.tsid.compareTo(b.tsid);
                if (res == 0) {
                    return a.timestamp < b.timestamp;
                } else {
                    return res < 0;
                }
            }
        };
        for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
            LeafCollector leafCollector = queryCollector.getLeafCollector(leaf);
            Scorer scorer = weight.scorer(leaf);
            if (scorer != null) {
                LeafWalker walker = new LeafWalker(leaf, scorer, leafCollector);
                if (walker.next()) {
                    queue.add(walker);
                }
            }
        }
        while (queue.top() != null) {
            LeafWalker walker = queue.top();
            walker.collectCurrent();
            if (walker.next()) {
                queue.updateTop();
            } else {
                queue.pop();
            }
        }
    }

    private static class LeafWalker {
        private final LeafCollector collector;
        private final Bits liveDocs;
        private final DocIdSetIterator iterator;
        private final SortedSetDocValues tsids;
        private final SortedNumericDocValues timestamps;
        int docBase;
        int docId;
        BytesRef tsid;
        Long timestamp;

        String id;

        LeafWalker(LeafReaderContext context, Scorer scorer, LeafCollector collector) throws IOException {
            id = context.toString().replaceFirst(".*:c", "c");
            id = id.substring(0, id.indexOf(':'));

            this.collector = collector;
            liveDocs = context.reader().getLiveDocs();
            this.collector.setScorer(scorer);
            iterator = scorer.iterator();
            docBase = context.docBase;
            tsids = context.reader().getSortedSetDocValues(TimeSeriesIdFieldMapper.NAME);
            timestamps = context.reader().getSortedNumericDocValues(DataStream.TimestampField.FIXED_TIMESTAMP_FIELD);
        }

        void collectCurrent() throws IOException {
            collector.collect(docId);
        }

        boolean next() {
            try {
                do {
                    docId = iterator.nextDoc();
                    if (docId != DocIdSetIterator.NO_MORE_DOCS && (liveDocs == null || liveDocs.get(docId))) {
                        if (tsids.advanceExact(docId)) {
                            BytesRef tsid = tsids.lookupOrd(tsids.nextOrd());
                            if (timestamps.advanceExact(docId)) {
                                this.timestamp = timestamps.nextValue();
                                if (tsid.equals(this.tsid) == false) {
                                    this.tsid = BytesRef.deepCopyOf(tsid);
                                }
                                return true;
                            }
                        }
                    }
                } while (docId != DocIdSetIterator.NO_MORE_DOCS);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            this.tsid = null;
            this.timestamp = null;
            return false;
        }
    }
}
