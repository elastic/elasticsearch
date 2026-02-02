/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ConstantScoreScorerSupplier;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;

class MatchTailTimestampSortedDocsQuery extends Query {

    private final int size;

    MatchTailTimestampSortedDocsQuery(int size) {
        this.size = size;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }

            @Override
            public int count(LeafReaderContext context) throws IOException {
                return context.reader().numDocs();
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                if (segmentSortedByDescTimestamp(context)) {
                    int start = context.reader().numDocs() - size - 1;
                    int maxDoc = context.reader().maxDoc();
                    return ConstantScoreScorerSupplier.fromIterator(DocIdSetIterator.range(start, maxDoc), boost, scoreMode, maxDoc);
                } else {
                    return ConstantScoreScorerSupplier.matchAll(boost, scoreMode, context.reader().maxDoc());
                }
            }
        };
    }

    @Override
    public String toString(String field) {
        return "MatchTailTimestampSortedDocsQuery{size=" + size + '}';
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MatchTailTimestampSortedDocsQuery that) {
            return size == that.size;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(size);
    }

    private static boolean segmentSortedByDescTimestamp(LeafReaderContext context) throws IOException {
        Sort sort = context.reader().getMetaData().sort();
        if (sort == null) {
            return false;
        }
        for (SortField sortField : sort.getSort()) {
            if ("@timestamp".equals(sortField.getField())) {
                return sortField.getReverse() == false;
            }
            if (isSingletonInSegment(context, sortField)) {
                continue;
            }
            return false;
        }
        return false;
    }

    private static boolean isSingletonInSegment(LeafReaderContext context, SortField sortField) throws IOException {
        if (Objects.equals(sortField.getField(), "host.name") == false) {
            return false;
        }
        SortedSetDocValues ssdv = context.reader().getSortedSetDocValues("host.name");
        if (ssdv == null) {
            return true;
        }
        return (ssdv.getValueCount() == 1);
    }
}
