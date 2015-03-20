/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.percolator;

import com.google.common.collect.Maps;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fieldvisitor.JustSourceFieldsVisitor;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.util.Map;

/**
 */
final class QueriesLoaderCollector extends SimpleCollector {

    private final Map<BytesRef, Query> queries = Maps.newHashMap();
    private final JustSourceFieldsVisitor fieldsVisitor = new JustSourceFieldsVisitor();
    private final PercolatorQueriesRegistry percolator;
    private final IndexFieldData<?> uidFieldData;
    private final ESLogger logger;

    private SortedBinaryDocValues uidValues;
    private LeafReader reader;

    QueriesLoaderCollector(PercolatorQueriesRegistry percolator, ESLogger logger, MapperService mapperService, IndexFieldDataService indexFieldDataService) {
        this.percolator = percolator;
        this.logger = logger;
        final FieldMapper<?> uidMapper = mapperService.smartNameFieldMapper(UidFieldMapper.NAME);
        this.uidFieldData = indexFieldDataService.getForField(uidMapper);
    }

    public Map<BytesRef, Query> queries() {
        return this.queries;
    }

    @Override
    public void collect(int doc) throws IOException {
        // the _source is the query

        uidValues.setDocument(doc);
        if (uidValues.count() > 0) {
            assert uidValues.count() == 1;
            final BytesRef uid = uidValues.valueAt(0);
            final BytesRef id = Uid.splitUidIntoTypeAndId(uid)[1];
            fieldsVisitor.reset();
            reader.document(doc, fieldsVisitor);

            try {
                // id is only used for logging, if we fail we log the id in the catch statement
                final Query parseQuery = percolator.parsePercolatorDocument(null, fieldsVisitor.source());
                if (parseQuery != null) {
                    queries.put(BytesRef.deepCopyOf(id), parseQuery);
                } else {
                    logger.warn("failed to add query [{}] - parser returned null", id);
                }

            } catch (Exception e) {
                logger.warn("failed to add query [{}]", e, id.utf8ToString());
            }
        }
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
        reader = context.reader();
        uidValues = uidFieldData.load(context).getBytesValues();
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    @Override
    public boolean needsScores() {
        return false;
    }
}
