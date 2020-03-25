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
package org.apache.lucene.queries;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.BaseRangeFieldQueryTestCase;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.mapper.RangeType;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

import static org.apache.lucene.queries.BinaryDocValuesRangeQuery.QueryType.CONTAINS;
import static org.apache.lucene.queries.BinaryDocValuesRangeQuery.QueryType.CROSSES;
import static org.apache.lucene.queries.BinaryDocValuesRangeQuery.QueryType.INTERSECTS;
import static org.apache.lucene.queries.BinaryDocValuesRangeQuery.QueryType.WITHIN;

public abstract class BaseRandomBinaryDocValuesRangeQueryTestCase extends BaseRangeFieldQueryTestCase {

    @Override
    public void testMultiValued() throws Exception {
        // Can't test this how BaseRangeFieldQueryTestCase works now, because we're using BinaryDocValuesField here.
    }

    @Override
    protected final Field newRangeField(Range box) {
        AbstractRange<?> testRange = (AbstractRange<?>) box;
        RangeFieldMapper.Range range = new RangeFieldMapper.Range(rangeType(), testRange.getMin(), testRange.getMax(), true , true);
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
