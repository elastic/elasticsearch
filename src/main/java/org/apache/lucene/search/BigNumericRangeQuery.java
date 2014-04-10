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

package org.apache.lucene.search;

import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.*;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.LinkedList;

/**
 * Copies from {@link org.apache.lucene.search.NumericRangeQuery} to support {@link java.math.BigInteger}
 */
public class BigNumericRangeQuery<T extends Number> extends MultiTermQuery {
    private BigNumericRangeQuery(final String field, final int precisionStep, final BigNumericUtils.BigNumericType dataType,
                              T min, T max, final boolean minInclusive, final boolean maxInclusive, final int valueSize
    ) {
        super(field);
        if (precisionStep < 1)
            throw new IllegalArgumentException("precisionStep must be >=1");
        this.precisionStep = precisionStep;
        this.dataType = dataType;
        this.min = min;
        this.max = max;
        this.minInclusive = minInclusive;
        this.maxInclusive = maxInclusive;
        this.valueSize = valueSize;
    }


    public static BigNumericRangeQuery<BigInteger> newBigIntegerRange(final String field, final int precisionStep,
                                                                      BigInteger min, BigInteger max, final boolean minInclusive, final boolean maxInclusive, final int valueSize
    ) {
        return new BigNumericRangeQuery<BigInteger>(field, precisionStep, BigNumericUtils.BigNumericType.BIG_INT, min, max, minInclusive, maxInclusive, valueSize);
    }


    @Override
    @SuppressWarnings("unchecked")
    protected TermsEnum getTermsEnum(final Terms terms, AttributeSource atts) throws IOException {
        // very strange: java.lang.Number itself is not Comparable, but all subclasses used here are
        if (min != null && max != null && ((Comparable<T>) min).compareTo(max) > 0) {
            return TermsEnum.EMPTY;
        }
        return new NumericRangeTermsEnum(terms.iterator(null));
    }

    /** Returns <code>true</code> if the lower endpoint is inclusive */
    public boolean includesMin() { return minInclusive; }

    /** Returns <code>true</code> if the upper endpoint is inclusive */
    public boolean includesMax() { return maxInclusive; }

    /** Returns the lower value of this range query */
    public T getMin() { return min; }

    /** Returns the upper value of this range query */
    public T getMax() { return max; }

    /** Returns the precision step. */
    public int getPrecisionStep() { return precisionStep; }

    @Override
    public String toString(final String field) {
        final StringBuilder sb = new StringBuilder();
        if (!getField().equals(field)) sb.append(getField()).append(':');
        return sb.append(minInclusive ? '[' : '{')
                .append((min == null) ? "*" : min.toString())
                .append(" TO ")
                .append((max == null) ? "*" : max.toString())
                .append(maxInclusive ? ']' : '}')
                .append(ToStringUtils.boost(getBoost()))
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        BigNumericRangeQuery that = (BigNumericRangeQuery) o;

        if (maxInclusive != that.maxInclusive) return false;
        if (minInclusive != that.minInclusive) return false;
        if (precisionStep != that.precisionStep) return false;
        if (valueSize != that.valueSize) return false;
        if (max != null ? !max.equals(that.max) : that.max != null) return false;
        if (min != null ? !min.equals(that.min) : that.min != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + precisionStep;
        result = 31 * result + (min != null ? min.hashCode() : 0);
        result = 31 * result + (max != null ? max.hashCode() : 0);
        result = 31 * result + (minInclusive ? 1 : 0);
        result = 31 * result + (maxInclusive ? 1 : 0);
        result = 31 * result + valueSize;
        return result;
    }

    final int precisionStep;
    final BigNumericUtils.BigNumericType dataType;
    final T min, max;
    final boolean minInclusive,maxInclusive;
    final int valueSize;


    static final BigInteger MINUS_ONE = BigInteger.valueOf(-1);


    private final class NumericRangeTermsEnum extends FilteredTermsEnum {

        private BytesRef currentLowerBound, currentUpperBound;

        private final LinkedList<BytesRef> rangeBounds = new LinkedList<BytesRef>();
        private final Comparator<BytesRef> termComp;

        NumericRangeTermsEnum(final TermsEnum tenum) {
            super(tenum);
            switch (dataType) {
                case BIG_INT:
                    BigInteger minBound = min == null? MINUS_ONE.shiftLeft(valueSize - 1) : (BigInteger) min;  // min value is -2 ^ (valueSize -1)
                    if (!minInclusive) {
                        minBound = minBound.add(BigInteger.ONE);
                    }
                    BigInteger maxBound = max == null ? BigInteger.ONE.shiftLeft(valueSize - 1).subtract(BigInteger.ONE) : (BigInteger) max;  // max value is 2 ^ (valueSize -1) - 1
                    if (!maxInclusive) {
                        maxBound = maxBound.subtract(BigInteger.ONE);
                    }

                    BigNumericUtils.splitRange(new BigNumericUtils.BigIntegerRangeBuilder() {
                        @Override
                        public final void addRange(BytesRef minPrefixCoded, BytesRef maxPrefixCoded) {
                            rangeBounds.add(minPrefixCoded);
                            rangeBounds.add(maxPrefixCoded);
                        }
                    }, valueSize, precisionStep, minBound, maxBound);
                    break;
                default:
                    // should never happen
                    throw new IllegalArgumentException("Invalid NumericType");
            }

            termComp = getComparator();
        }

        private void nextRange() {
            assert rangeBounds.size() % 2 == 0;

            currentLowerBound = rangeBounds.removeFirst();
            assert currentUpperBound == null || termComp.compare(currentUpperBound, currentLowerBound) <= 0 :
                    "The current upper bound must be <= the new lower bound";

            currentUpperBound = rangeBounds.removeFirst();
        }

        @Override
        protected final BytesRef nextSeekTerm(BytesRef term) {
            while (rangeBounds.size() >= 2) {
                nextRange();

                // if the new upper bound is before the term parameter, the sub-range is never a hit
                if (term != null && termComp.compare(term, currentUpperBound) > 0)
                    continue;
                // never seek backwards, so use current term if lower bound is smaller
                return (term != null && termComp.compare(term, currentLowerBound) > 0) ?
                        term : currentLowerBound;
            }

            // no more sub-range enums available
            assert rangeBounds.isEmpty();
            currentLowerBound = currentUpperBound = null;
            return null;
        }

        @Override
        protected final AcceptStatus accept(BytesRef term) {
            while (currentUpperBound == null || termComp.compare(term, currentUpperBound) > 0) {
                if (rangeBounds.isEmpty())
                    return AcceptStatus.END;
                // peek next sub-range, only seek if the current term is smaller than next lower bound
                if (termComp.compare(term, rangeBounds.getFirst()) < 0)
                    return AcceptStatus.NO_AND_SEEK;
                // step forward to next range without seeking, as next lower range bound is less or equal current term
                nextRange();
            }
            return AcceptStatus.YES;
        }

    }


}
