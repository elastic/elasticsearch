package org.elasticsearch.index.percolator;

import com.google.common.collect.Maps;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.fieldvisitor.UidAndSourceFieldsVisitor;

import java.io.IOException;
import java.util.Map;

/**
 */
final class QueriesLoaderCollector extends Collector {

    private final Map<Text, Query> queries = Maps.newHashMap();
    private final PercolatorQueriesRegistry percolator;
    private final ESLogger logger;

    private AtomicReader reader;

    QueriesLoaderCollector(PercolatorQueriesRegistry percolator, ESLogger logger) {
        this.percolator = percolator;
        this.logger = logger;
    }

    public Map<Text, Query> queries() {
        return this.queries;
    }

    @Override
    public void collect(int doc) throws IOException {
        // the _source is the query
        UidAndSourceFieldsVisitor fieldsVisitor = new UidAndSourceFieldsVisitor();
        reader.document(doc, fieldsVisitor);
        String id = fieldsVisitor.uid().id();
        try {
            final Query parseQuery = percolator.parsePercolatorDocument(id, fieldsVisitor.source());
            if (parseQuery != null) {
                queries.put(new BytesText(new HashedBytesArray(Strings.toUTF8Bytes(id))), parseQuery);
            } else {
                logger.warn("failed to add query [{}] - parser returned null", id);
            }

        } catch (Exception e) {
            logger.warn("failed to add query [{}]", e, id);
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.reader = context.reader();
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }
}
