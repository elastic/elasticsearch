/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.ConstantScoreScorerSupplier;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.elasticsearch.cluster.metadata.DataStream;

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongPredicate;

public final class TimestampQuery extends Query {

    private static final String FIELD_NAME = DataStream.TIMESTAMP_FIELD_NAME;

    private final long minTimestamp;
    private final long maxTimestamp;

    public TimestampQuery(long minTimestamp, long maxTimestamp) {
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        if (minTimestamp == Long.MIN_VALUE && maxTimestamp == Long.MAX_VALUE) {
            return new FieldExistsQuery(FIELD_NAME);
        }
        if (minTimestamp > maxTimestamp) {
            return new MatchNoDocsQuery();
        }
        return super.rewrite(indexSearcher);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                var reader = context.reader();
                int maxDoc = reader.maxDoc();
                if (maxDoc == 0) {
                    return null;
                }
                if (reader.getFieldInfos().fieldInfo(FIELD_NAME) == null) {
                    return null;
                }

                Sort indexSort = reader.getMetaData().sort();
                assert indexSort != null;
                assert indexSort.getSort().length == 1 || indexSort.getSort().length == 2;

                var timestampSkipper = reader.getDocValuesSkipper(FIELD_NAME);
                assert timestampSkipper != null;
                if (timestampSkipper.minValue() > maxTimestamp || timestampSkipper.maxValue() < minTimestamp) {
                    return null;
                }
                if (timestampSkipper.minValue() >= minTimestamp && timestampSkipper.maxValue() <= maxTimestamp) {
                    return ConstantScoreScorerSupplier.matchAll(score(), scoreMode, maxDoc);
                }

                var timestamps = getNumericDocValues(reader);
                String primarySortField = indexSort.getSort()[0].getField();
                boolean timestampIsPrimarySort = primarySortField.equals(FIELD_NAME);
                var primaryFieldValues = timestampIsPrimarySort ? null : reader.getSortedDocValues(primarySortField);
                if (primaryFieldValues == null || primaryFieldValues.getValueCount() <= 1) {
                    var iterator = getIteratorIfTimestampIfPrimarySort(maxDoc, timestamps, timestampSkipper, minTimestamp, maxTimestamp);
                    return ConstantScoreScorerSupplier.fromIterator(iterator, score(), scoreMode, maxDoc);
                }
                var primaryFieldSkipper = reader.getDocValuesSkipper(primarySortField);
                var iterator = new TimestampTwoPhaseIterator(timestamps, timestampSkipper, primaryFieldSkipper, minTimestamp, maxTimestamp);
                return ConstantScoreScorerSupplier.fromIterator(TwoPhaseIterator.asDocIdSetIterator(iterator), score(), scoreMode, maxDoc);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                // timestamp field isn't updatable
                return true;
            }

        };
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(FIELD_NAME)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public String toString(String field) {
        StringBuilder b = new StringBuilder();
        if (FIELD_NAME.equals(field) == false) {
            b.append(FIELD_NAME).append(":");
        }
        return b.append("[").append(minTimestamp).append(" TO ").append(maxTimestamp).append("]").toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        var that = (TimestampQuery) obj;
        return minTimestamp == that.minTimestamp && maxTimestamp == that.maxTimestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), minTimestamp, maxTimestamp);
    }

    static NumericDocValues getNumericDocValues(LeafReader reader) throws IOException {
        var timestampValues = DocValues.getSortedNumeric(reader, FIELD_NAME);
        assert timestampValues != null;
        var timestampSingleton = DocValues.unwrapSingleton(timestampValues);
        assert timestampSingleton != null : "@timestamp has multiple values per document";
        return timestampSingleton;
    }

    // Based on SortedNumericDocValuesRangeQuery#getDocIdSetIteratorOrNullForPrimarySort(...) but adapted for logsdb / tsdb @timestamp field
    // use case.
    static DocIdSetIterator getIteratorIfTimestampIfPrimarySort(
        int maxDoc,
        NumericDocValues timestamps,
        DocValuesSkipper timestampFieldSkipper,
        long minTimestamp,
        long maxTimestamp
    ) throws IOException {
        assert timestampFieldSkipper.docCount() == maxDoc;

        final int minDocID;
        if (timestampFieldSkipper.maxValue() <= maxTimestamp) {
            minDocID = 0;
        } else {
            timestampFieldSkipper.advance(Long.MIN_VALUE, maxTimestamp);
            minDocID = nextDoc(timestampFieldSkipper.minDocID(0), timestamps, l -> l <= maxTimestamp);
        }
        final int maxDocID;
        if (timestampFieldSkipper.minValue() >= minTimestamp) {
            maxDocID = timestampFieldSkipper.docCount();
        } else {
            timestampFieldSkipper.advance(Long.MIN_VALUE, minTimestamp);
            maxDocID = nextDoc(timestampFieldSkipper.minDocID(0), timestamps, l -> l < minTimestamp);
        }
        return minDocID == maxDocID ? DocIdSetIterator.empty() : DocIdSetIterator.range(minDocID, maxDocID);
    }

    private static int nextDoc(int startDoc, NumericDocValues docValues, LongPredicate predicate) throws IOException {
        int doc = docValues.docID();
        if (startDoc > doc) {
            doc = docValues.advance(startDoc);
        }
        for (; doc < DocIdSetIterator.NO_MORE_DOCS; doc = docValues.nextDoc()) {
            if (predicate.test(docValues.longValue())) {
                break;
            }
        }
        return doc;
    }

}
