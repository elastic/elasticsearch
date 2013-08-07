package org.elasticsearch.percolator;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 */
abstract class QueryCollector extends Collector {

    final IndexFieldData uidFieldData;
    final IndexSearcher searcher;
    final ConcurrentMap<Text, Query> queries;
    final ESLogger logger;

    final Lucene.ExistsCollector collector = new Lucene.ExistsCollector();

    BytesValues values;

    QueryCollector(ESLogger logger, ConcurrentMap<Text, Query> queries, IndexSearcher searcher, IndexFieldDataService fieldData) {
        this.logger = logger;
        this.queries = queries;
        this.searcher = searcher;
        // TODO: when we move to a UID level mapping def on the index level, we can use that one, now, its per type, and we can't easily choose one
        this.uidFieldData = fieldData.getForField(new FieldMapper.Names(UidFieldMapper.NAME), new FieldDataType("string", ImmutableSettings.builder().put("format", "paged_bytes")));
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        // we use the UID because id might not be indexed
        values = uidFieldData.load(context).getBytesValues();
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }


    static Match match(ESLogger logger, ConcurrentMap<Text, Query> queries, IndexSearcher searcher, IndexFieldDataService fieldData, PercolatorService.PercolateContext context) {
        return new Match(logger, queries, searcher, fieldData, context);
    }

    static Count count(ESLogger logger, ConcurrentMap<Text, Query> queries, IndexSearcher searcher, IndexFieldDataService fieldData) {
        return new Count(logger, queries, searcher, fieldData);
    }

    final static class Match extends QueryCollector {

        private final List<Text> matches = new ArrayList<Text>();
        private final boolean limit;
        private final int size;
        private long counter = 0;

        Match(ESLogger logger, ConcurrentMap<Text, Query> queries, IndexSearcher searcher, IndexFieldDataService fieldData, PercolatorService.PercolateContext context) {
            super(logger, queries, searcher, fieldData);
            this.limit = context.limit;
            this.size = context.size;
        }

        @Override
        public void collect(int doc) throws IOException {
            BytesRef uid = values.getValue(doc);
            if (uid == null) {
                return;
            }
            Text id = new BytesText(Uid.idFromUid(uid));
            Query query = queries.get(id);
            if (query == null) {
                // log???
                return;
            }
            // run the query
            try {
                collector.reset();
                searcher.search(query, collector);
                if (collector.exists()) {
                    if (!limit) {
                        matches.add(id);
                    } else if (counter < size) {
                        matches.add(id);
                    }
                    counter++;
                }
            } catch (IOException e) {
                logger.warn("[" + id + "] failed to execute query", e);
            }
        }

        long counter() {
            return counter;
        }

        List<Text> matches() {
            return matches;
        }

    }

    final static class Count extends QueryCollector {

        private long counter = 0;

        Count(ESLogger logger, ConcurrentMap<Text, Query> queries, IndexSearcher searcher, IndexFieldDataService fieldData) {
            super(logger, queries, searcher, fieldData);
        }

        @Override
        public void collect(int doc) throws IOException {
            BytesRef uid = values.getValue(doc);
            if (uid == null) {
                return;
            }
            Text id = new BytesText(Uid.idFromUid(uid));
            Query query = queries.get(id);
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
                logger.warn("[" + id + "] failed to execute query", e);
            }
        }

        long counter() {
            return counter;
        }

    }

}
