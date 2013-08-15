package org.elasticsearch.percolator;

import gnu.trove.list.array.TFloatArrayList;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.internal.IdFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 */
abstract class QueryCollector extends Collector {

    final IndexFieldData idFieldData;
    final IndexSearcher searcher;
    final ConcurrentMap<HashedBytesRef, Query> queries;
    final ESLogger logger;

    final Lucene.ExistsCollector collector = new Lucene.ExistsCollector();
    final HashedBytesRef spare = new HashedBytesRef(new BytesRef());

    BytesValues values;

    QueryCollector(ESLogger logger, PercolatorService.PercolateContext context) {
        this.logger = logger;
        this.queries = context.percolateQueries;
        this.searcher = context.docSearcher;
        this.idFieldData = context.fieldData.getForField(
                new FieldMapper.Names(IdFieldMapper.NAME),
                new FieldDataType("string", ImmutableSettings.builder().put("format", "paged_bytes"))
        );
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        // we use the UID because id might not be indexed
        values = idFieldData.load(context).getBytesValues();
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }


    static Match match(ESLogger logger, PercolatorService.PercolateContext context) {
        return new Match(logger, context);
    }

    static Count count(ESLogger logger, PercolatorService.PercolateContext context) {
        return new Count(logger, context);
    }

    static MatchAndScore matchAndScore(ESLogger logger, PercolatorService.PercolateContext context) {
        return new MatchAndScore(logger, context);
    }

    static MatchAndSort matchAndSort(ESLogger logger, PercolatorService.PercolateContext context) {
        return new MatchAndSort(logger, context);
    }

    final static class Match extends QueryCollector {

        private final List<BytesRef> matches = new ArrayList<BytesRef>();
        private final boolean limit;
        private final int size;
        private long counter = 0;

        Match(ESLogger logger, PercolatorService.PercolateContext context) {
            super(logger, context);
            this.limit = context.limit;
            this.size = context.size;
        }

        @Override
        public void collect(int doc) throws IOException {
            spare.hash = values.getValueHashed(doc, spare.bytes);
            Query query = queries.get(spare);
            if (query == null) {
                // log???
                return;
            }
            // run the query
            try {
                collector.reset();
                searcher.search(query, collector);
                if (collector.exists()) {
                    if (!limit || counter < size) {
                        matches.add(values.makeSafe(spare.bytes));
                    }
                    counter++;
                }
            } catch (IOException e) {
                logger.warn("[" + spare.bytes.utf8ToString() + "] failed to execute query", e);
            }
        }

        long counter() {
            return counter;
        }

        List<BytesRef> matches() {
            return matches;
        }

    }

    final static class MatchAndSort extends QueryCollector {

        private final TopScoreDocCollector topDocsCollector;

        MatchAndSort(ESLogger logger, PercolatorService.PercolateContext context) {
            super(logger, context);
            // TODO: Use TopFieldCollector.create(...) for ascending and decending scoring?
            topDocsCollector = TopScoreDocCollector.create(context.size, false);
        }

        @Override
        public void collect(int doc) throws IOException {
            spare.hash = values.getValueHashed(doc, spare.bytes);
            Query query = queries.get(spare);
            if (query == null) {
                // log???
                return;
            }
            // run the query
            try {
                collector.reset();
                searcher.search(query, collector);
                if (collector.exists()) {
                    topDocsCollector.collect(doc);
                }
            } catch (IOException e) {
                logger.warn("[" + spare.bytes.utf8ToString() + "] failed to execute query", e);
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            super.setNextReader(context);
            topDocsCollector.setNextReader(context);
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            topDocsCollector.setScorer(scorer);
        }

        TopDocs topDocs() {
            return topDocsCollector.topDocs();
        }

    }

    final static class MatchAndScore extends QueryCollector {

        private final List<BytesRef> matches = new ArrayList<BytesRef>();
        // TODO: Use thread local in order to cache the scores lists?
        private final TFloatArrayList scores = new TFloatArrayList();
        private final boolean limit;
        private final int size;
        private long counter = 0;

        private Scorer scorer;

        MatchAndScore(ESLogger logger, PercolatorService.PercolateContext context) {
            super(logger, context);
            this.limit = context.limit;
            this.size = context.size;
        }

        @Override
        public void collect(int doc) throws IOException {
            spare.hash = values.getValueHashed(doc, spare.bytes);
            Query query = queries.get(spare);
            if (query == null) {
                // log???
                return;
            }
            // run the query
            try {
                collector.reset();
                searcher.search(query, collector);
                if (collector.exists()) {
                    if (!limit || counter < size) {
                        matches.add(values.makeSafe(spare.bytes));
                        scores.add(scorer.score());
                    }
                    counter++;
                }
            } catch (IOException e) {
                logger.warn("[" + spare.bytes.utf8ToString() + "] failed to execute query", e);
            }
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
        }

        long counter() {
            return counter;
        }

        List<BytesRef> matches() {
            return matches;
        }

        TFloatArrayList scores() {
            return scores;
        }
    }

    final static class Count extends QueryCollector {

        private long counter = 0;

        Count(ESLogger logger, PercolatorService.PercolateContext context) {
            super(logger, context);
        }

        @Override
        public void collect(int doc) throws IOException {
            spare.hash = values.getValueHashed(doc, spare.bytes);
            Query query = queries.get(spare);
            if (query == null) {
                // log???
                return;
            }
            // run the query
            try {
                collector.reset();
                searcher.search(query, collector);
                if (collector.exists()) {
                    counter++;
                }
            } catch (IOException e) {
                logger.warn("[" + spare.bytes.utf8ToString() + "] failed to execute query", e);
            }
        }

        long counter() {
            return counter;
        }

    }

}
