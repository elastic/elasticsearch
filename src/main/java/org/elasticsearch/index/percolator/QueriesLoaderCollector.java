package org.elasticsearch.index.percolator;

import com.google.common.collect.Maps;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fieldvisitor.JustSourceFieldsVisitor;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.IdFieldMapper;

import java.io.IOException;
import java.util.Map;

/**
 */
final class QueriesLoaderCollector extends Collector {

    private final Map<HashedBytesRef, Query> queries = Maps.newHashMap();
    private final JustSourceFieldsVisitor fieldsVisitor = new JustSourceFieldsVisitor();
    private final PercolatorQueriesRegistry percolator;
    private final IndexFieldData idFieldData;
    private final ESLogger logger;

    private BytesValues idValues;
    private AtomicReader reader;

    QueriesLoaderCollector(PercolatorQueriesRegistry percolator, ESLogger logger, MapperService mapperService, IndexFieldDataService indexFieldDataService) {
        this.percolator = percolator;
        this.logger = logger;
        final FieldMapper<?> idMapper = mapperService.smartNameFieldMapper(IdFieldMapper.NAME);
        this.idFieldData = indexFieldDataService.getForField(idMapper);
    }

    public Map<HashedBytesRef, Query> queries() {
        return this.queries;
    }

    @Override
    public void collect(int doc) throws IOException {
        // the _source is the query

        if (idValues.setDocument(doc) > 0) {
            BytesRef id = idValues.nextValue();
            fieldsVisitor.reset();
            reader.document(doc, fieldsVisitor);

            try {
                // id is only used for logging, if we fail we log the id in the catch statement
                final Query parseQuery = percolator.parsePercolatorDocument(null, fieldsVisitor.source());
                if (parseQuery != null) {
                    queries.put(new HashedBytesRef(idValues.copyShared(), idValues.currentValueHash()), parseQuery);
                } else {
                    logger.warn("failed to add query [{}] - parser returned null", id);
                }

            } catch (Exception e) {
                logger.warn("failed to add query [{}]", e, id.utf8ToString());
            }
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        reader = context.reader();
        idValues = idFieldData.load(context).getBytesValues(true);
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }
}
