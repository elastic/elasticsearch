/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.lucene.queries;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Query;
import org.apache.lucene.tests.search.BaseRangeFieldQueryTestCase;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.mapper.RangeType;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

import static org.elasticsearch.lucene.queries.BinaryDocValuesRangeQuery.QueryType.CONTAINS;
import static org.elasticsearch.lucene.queries.BinaryDocValuesRangeQuery.QueryType.CROSSES;
import static org.elasticsearch.lucene.queries.BinaryDocValuesRangeQuery.QueryType.INTERSECTS;
import static org.elasticsearch.lucene.queries.BinaryDocValuesRangeQuery.QueryType.WITHIN;

public abstract class BaseRandomBinaryDocValuesRangeQueryTestCase extends BaseRangeFieldQueryTestCase {

    @Override
    public void testMultiValued() throws Exception {
        // Can't test this how BaseRangeFieldQueryTestCase works now, because we're using BinaryDocValuesField here.
    }

    @Override
    protected final Field newRangeField(Range box) {
        AbstractRange<?> testRange = (AbstractRange<?>) box;
        RangeFieldMapper.Range range = new RangeFieldMapper.Range(rangeType(), testRange.getMin(), testRange.getMax(), true, true);
        try {
            BytesRef encodeRange = rangeType().encodeRanges(Collections.singleton(range));
            return new BinaryDocValuesField(fieldName(), encodeRange);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected final Query newIntersectsQuery(Range box) {
        AbstractRange<?> testRange = (AbstractRange<?>) box;
        return rangeType().dvRangeQuery(fieldName(), INTERSECTS, testRange.getMin(), testRange.getMax(), true, true);
    }

    @Override
    protected final Query newContainsQuery(Range box) {
        AbstractRange<?> testRange = (AbstractRange<?>) box;
        return rangeType().dvRangeQuery(fieldName(), CONTAINS, testRange.getMin(), testRange.getMax(), true, true);
    }

    @Override
    protected final Query newWithinQuery(Range box) {
        AbstractRange<?> testRange = (AbstractRange<?>) box;
        return rangeType().dvRangeQuery(fieldName(), WITHIN, testRange.getMin(), testRange.getMax(), true, true);
    }

    @Override
    protected final Query newCrossesQuery(Range box) {
        AbstractRange<?> testRange = (AbstractRange<?>) box;
        return rangeType().dvRangeQuery(fieldName(), CROSSES, testRange.getMin(), testRange.getMax(), true, true);
    }

    @Override
    protected final int dimension() {
        return 1;
    }

    protected abstract String fieldName();

    protected abstract RangeType rangeType();

    protected abstract static class AbstractRange<T> extends Range {

        protected final int numDimensions() {
            return 1;
        }

        @Override
        protected final Object getMin(int dim) {
            assert dim == 0;
            return getMin();
        }

        public abstract T getMin();

        @Override
        protected final Object getMax(int dim) {
            assert dim == 0;
            return getMax();
        }

        public abstract T getMax();

        @Override
        protected final boolean isEqual(Range o) {
            AbstractRange<?> other = (AbstractRange<?>) o;
            return Objects.equals(getMin(), other.getMin()) && Objects.equals(getMax(), other.getMax());
        }

        @Override
        public final String toString() {
            return "Box(" + getMin() + " TO " + getMax() + ")";
        }
    }

}
