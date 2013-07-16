package org.elasticsearch.index.percolator;

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
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 */
final class QueryCollector extends Collector {

    private final IndexFieldData uidFieldData;
    private final IndexSearcher searcher;
    private final List<Text> matches;
    private final ConcurrentMap<Text, Query> queries;
    private final ESLogger logger;

    private final Lucene.ExistsCollector collector = new Lucene.ExistsCollector();

    private BytesValues values;

    QueryCollector(ESLogger logger, ConcurrentMap<Text, Query> queries, IndexSearcher searcher, IndexFieldDataService fieldData, List<Text> matches) {
        this.logger = logger;
        this.queries = queries;
        this.searcher = searcher;
        this.matches = matches;
        // TODO: when we move to a UID level mapping def on the index level, we can use that one, now, its per type, and we can't easily choose one
        this.uidFieldData = fieldData.getForField(new FieldMapper.Names(UidFieldMapper.NAME), new FieldDataType("string", ImmutableSettings.builder().put("format", "paged_bytes")));
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
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
                matches.add(id);
            }
        } catch (IOException e) {
            logger.warn("[" + id + "] failed to execute query", e);
        }
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
}
