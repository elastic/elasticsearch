/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.queryableexpression;

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.core.SuppressForbidden;

import java.util.function.LongFunction;

/**
 * Approximates the reversal of two's complement {@code long} operations
 * into Lucene {@link Query queries}.
 */
abstract class AbstractLongQueryableExpression implements LongQueryableExpression {
    @Override
    public QueryableExpression mapNumber(MapNumber map) {
        return map.withLong(this);
    }

    @Override
    public final QueryableExpression add(QueryableExpression rhs) {
        return add(rhs.castToLong());
    }

    protected abstract QueryableExpression add(LongQueryableExpression rhs);

    @Override
    public final QueryableExpression multiply(QueryableExpression rhs) {
        return multiply(rhs.castToLong());
    }

    protected abstract QueryableExpression multiply(LongQueryableExpression rhs);

    @Override
    public final QueryableExpression divide(QueryableExpression rhs) {
        return divide(rhs.castToLong());
    }

    protected abstract QueryableExpression divide(LongQueryableExpression rhs);

    @Override
    public LongQueryableExpression castToLong() {
        return this;
    }

    @Override
    public StringQueryableExpression castToString() {
        return UnqueryableExpression.UNQUERYABLE;
    }

    @Override
    public abstract String toString();

    /**
     * A queryable field who's values are exposed to the script as {@code long}s.
     */
    static class Field extends AbstractLongQueryableExpression {
        private final String name;
        private final LongQueries queries;

        Field(String name, LongQueries queries) {
            this.name = name;
            this.queries = queries;
        }

        @Override
        public Query approximateTermQuery(long term) {
            return queries.approximateTermQuery(term);
        }

        @Override
        public Query approximateRangeQuery(long lower, long upper) {
            return queries.approximateRangeQuery(lower, upper);
        }

        @Override
        public Query approximateExists() {
            return queries.approximateExists();
        }

        @Override
        public final QueryableExpression mapConstant(LongFunction<QueryableExpression> map) {
            return UnqueryableExpression.UNQUERYABLE;
        }

        @Override
        public QueryableExpression unknownOp() {
            return new UnknownOperationExpression(this);
        }

        @Override
        public QueryableExpression unknownOp(QueryableExpression rhs) {
            return new UnknownOperationExpression(this, rhs);
        }

        @Override
        protected QueryableExpression add(LongQueryableExpression rhs) {
            return rhs.mapConstant(c -> c == 0 ? this : new Add(this, c));
        }

        @Override
        protected QueryableExpression multiply(LongQueryableExpression rhs) {
            return rhs.mapConstant(c -> {
                if (c == -1L) {
                    return new Negate(this);
                }
                if (c == 0L) {
                    return new Constant(0);
                }
                if (c == 1L) {
                    return this;
                }
                return new Multiply(this, c);
            });
        }

        @Override
        protected QueryableExpression divide(LongQueryableExpression rhs) {
            return rhs.mapConstant(c -> {
                if (c == -1L) {
                    return new Negate(this);
                }
                if (c == 0L) {
                    // The script is likely to return NaN, but we can't query for that.
                    return UnqueryableExpression.UNQUERYABLE;
                }
                if (c == 1L) {
                    return this;
                }
                return new Divide(this, c);
            });
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * Adapts {@link IntQueries} into {@link LongQueries}.
     */
    static class IntQueriesToLongQueries implements LongQueries {
        private final IntQueries queries;

        IntQueriesToLongQueries(IntQueries queries) {
            this.queries = queries;
        }

        @Override
        public Query approximateExists() {
            return queries.approximateExists();
        }

        @Override
        public Query approximateTermQuery(long term) {
            if (term < Integer.MIN_VALUE) {
                return new MatchNoDocsQuery("[" + term + "] less than all ints");
            }
            if (Integer.MAX_VALUE < term) {
                return new MatchNoDocsQuery("[" + term + "] greater than all ints");
            }
            return queries.approximateTermQuery((int) term);
        }

        @Override
        public Query approximateRangeQuery(long lower, long upper) {
            if (upper < Integer.MIN_VALUE) {
                return new MatchNoDocsQuery("[" + upper + "] less than all ints");
            }
            if (lower > Integer.MAX_VALUE) {
                return new MatchNoDocsQuery("[" + lower + "] greater than all ints");
            }
            return queries.approximateRangeQuery((int) Math.max(lower, Integer.MIN_VALUE), (int) Math.min(upper, Integer.MAX_VALUE));
        }
    }

    /**
     * Add a constant to an expression or subtract a constant from an
     * expression. This can precisely reverse two's complement addition.
     */
    static class Add extends Chain {
        private final long n;

        Add(LongQueryableExpression next, long n) {
            super(next);
            if (n == 0) {
                throw new IllegalArgumentException("Don't create Add nodes for identity");
            }
            this.n = n;
        }

        @Override
        protected QueryableExpression add(LongQueryableExpression rhs) {
            return rhs.mapConstant(c -> new Add(next, n + c));
        }

        @Override
        public Query approximateTermQuery(long term) {
            return next.approximateTermQuery(term - n);
        }

        @Override
        public Query approximateRangeQuery(long lower, long upper) {
            lower -= n;
            upper -= n;
            if (lower < upper) {
                return next.approximateRangeQuery(lower, upper);
            }
            BooleanQuery.Builder bool = new BooleanQuery.Builder();
            bool.add(next.approximateRangeQuery(Long.MIN_VALUE, upper), Occur.SHOULD);
            bool.add(next.approximateRangeQuery(lower, Long.MAX_VALUE), Occur.SHOULD);
            return bool.build();
        }

        @Override
        public String toString() {
            return next + " + " + n;
        }
    }

    /**
     * Flip the sign of a value
     */
    static class Negate extends Chain {
        Negate(LongQueryableExpression next) {
            super(next);
        }

        @Override
        public Query approximateTermQuery(long term) {
            return next.approximateTermQuery(-term);
        }

        @Override
        public Query approximateRangeQuery(long lower, long upper) {
            if (lower == upper) {
                return approximateTermQuery(upper);
            }
            if (lower != Long.MIN_VALUE) {
                return next.approximateRangeQuery(-upper, -lower);
            }
            if (upper == Long.MAX_VALUE) {
                return next.approximateRangeQuery(Long.MIN_VALUE, Long.MAX_VALUE);
            }
            BooleanQuery.Builder b = new BooleanQuery.Builder();
            b.add(next.approximateRangeQuery(-upper, Long.MAX_VALUE), Occur.SHOULD);
            b.add(next.approximateTermQuery(Long.MIN_VALUE), Occur.SHOULD);
            return b.build();
        }

        @Override
        public String toString() {
            return "-(" + next + ")";
        }
    }

    /**
     * Multiply an expression by a constant. Instead of trying to accurately
     * reverse overflows this matches all of the values that cause the
     * multiplication to overflow.
     */
    static class Multiply extends Chain {
        private final long n;

        Multiply(LongQueryableExpression next, long n) {
            super(next);
            if (n == 0) {
                throw new IllegalArgumentException("Use Negate for * -1");
            }
            if (n == 0) {
                throw new IllegalArgumentException("Don't create Multiply nodes 0");
            }
            if (n == 1) {
                throw new IllegalArgumentException("Don't create Multiply nodes for identity");
            }
            this.n = n;
        }

        @Override
        public Query approximateTermQuery(long term) {
            return withOverflow(next.approximateTermQuery(term / n));
        }

        @Override
        public Query approximateRangeQuery(long lower, long upper) {
            long scaledLower = lower / n;
            long scaledUpper = upper / n;
            return withOverflow(next.approximateRangeQuery(Math.min(scaledLower, scaledUpper), Math.max(scaledLower, scaledUpper)));
        }

        /**
         * Returns a query that matches the provided query or any overflows.
         * <p>
         * The current implementation just ORs the query with everything that
         * would overflow the long. It works, but its inefficient if they overlap.
         */
        private Query withOverflow(Query query) {
            BooleanQuery.Builder bool = new BooleanQuery.Builder();
            bool.add(query, Occur.SHOULD);
            long overflowFromMin = Long.MIN_VALUE / n;
            long overflowFromMax = Long.MAX_VALUE / n;
            bool.add(next.approximateRangeQuery(Math.max(overflowFromMin, overflowFromMax), Long.MAX_VALUE), Occur.SHOULD);
            bool.add(next.approximateRangeQuery(Long.MIN_VALUE, Math.min(overflowFromMin, overflowFromMax)), Occur.SHOULD);
            return bool.build();
        }

        @Override
        public String toString() {
            return next + " * " + n;
        }
    }

    /**
     * Divide an expression by a constant. This can accurately reverse the
     * division into range queries.
     */
    static class Divide extends Chain {
        private final long n;

        Divide(LongQueryableExpression next, long n) {
            super(next);
            if (n == -1) {
                throw new IllegalArgumentException("Use Negatate for / -1");
            }
            if (n == 0) {
                throw new IllegalArgumentException("Don't create Divide nodes for 0");
            }
            if (n == 1) {
                throw new IllegalArgumentException("Don't create Divide nodes for identity");
            }
            this.n = n;
        }

        @Override
        public Query approximateTermQuery(long term) {
            /*
             * Division rounds to 0. So let's get 0 out of the way because
             * everything rounds towards it.
             */
            if (term == 0) {
                return next.approximateRangeQuery(-extraMagnitude(), extraMagnitude());
            }
            long precise = term * n;
            if (precise / n != term) {
                return new MatchNoDocsQuery("match would require a number that can't fit in long");
            }
            if (precise < 0) {
                /*
                 * Division always rounds towards 0. Here `precise` is a
                 * negative number, the largest one that when, divided by n,
                 * yields term. So we have to pick up all the numbers lower
                 * than it that also divide to term.
                 */
                long min = precise - extraMagnitude();
                if (min > precise) {
                    min = Long.MIN_VALUE;
                }
                return next.approximateRangeQuery(min, precise);
            }
            /*
             * Division always rounds towards 0. Here `precise` is a
             * positive number, the smallest one that when, divided by n,
             * yields term. So we have to pick up all the numbers lower
             * than it that also divide to term.
             */
            long max = precise + extraMagnitude();
            if (max < precise) {
                max = Long.MAX_VALUE;
            }
            return next.approximateRangeQuery(precise, max);
        }

        @Override
        public Query approximateRangeQuery(long lower, long upper) {
            long ln = clampedScale(lower);
            long un = clampedScale(upper);
            long min = Math.min(ln, un);
            long max = Math.max(ln, un);
            long extra = extraMagnitude();
            if (min <= 0) {
                min -= extra;
                if (min > 0) {
                    min = Long.MIN_VALUE;
                }
            }
            if (max >= 0) {
                max += extra;
                if (max < 0) {
                    max = Long.MAX_VALUE;
                }
            }
            return next.approximateRangeQuery(min, max);
        }

        @SuppressForbidden(reason = "abs is safe because we can't pass Long.MIN_VALUE")
        private long extraMagnitude() {
            return Math.abs(n - Long.signum(n));
        }

        private long clampedScale(long v) {
            long result = v * n;
            if (result / n != v) {
                /*
                 * This division is safe because we never build a divide node
                 * by with n=0. This wouldn't detect the overflow when v is
                 * Long.MIN_VALUE and n is -1, but we never build divide nodes
                 * with n=-1.
                 */
                return v < 0 ^ n < 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
            }
            return result;
        }

        @Override
        public String toString() {
            return next + " / " + n;
        }
    }

    /**
     * A constant {@link LongQueryableExpression}.
     */
    static class Constant extends AbstractLongQueryableExpression {
        private final long n;

        Constant(long n) {
            this.n = n;
        }

        @Override
        public QueryableExpression unknownOp() {
            return UNQUERYABLE;
        }

        @Override
        public QueryableExpression unknownOp(QueryableExpression rhs) {
            return rhs.unknownOp();
        }

        @Override
        public QueryableExpression add(LongQueryableExpression rhs) {
            QueryableExpression asConstant = rhs.mapConstant(c -> new Constant(n + c));
            if (asConstant != UnqueryableExpression.UNQUERYABLE) {
                return asConstant;
            }
            return rhs.add(this);
        }

        @Override
        public QueryableExpression multiply(LongQueryableExpression rhs) {
            QueryableExpression asConstant = rhs.mapConstant(c -> new Constant(n * c));
            if (asConstant != UnqueryableExpression.UNQUERYABLE) {
                return asConstant;
            }
            return rhs.multiply(this);
        }

        @Override
        public QueryableExpression divide(LongQueryableExpression rhs) {
            return rhs.mapConstant(c -> new Constant(n / c));
        }

        @Override
        public QueryableExpression mapConstant(LongFunction<QueryableExpression> map) {
            return map.apply(n);
        }

        @Override
        public Query approximateExists() {
            return new MatchAllDocsQuery();
        }

        @Override
        public Query approximateTermQuery(long term) {
            if (n == term) {
                return new MatchAllDocsQuery();
            }
            return new MatchNoDocsQuery("[" + term + "] != [" + n + "]");
        }

        @Override
        public Query approximateRangeQuery(long lower, long upper) {
            if (lower <= n && n <= upper) {
                return new MatchAllDocsQuery();
            }
            return new MatchNoDocsQuery("value [" + n + "] is not between [" + lower + "] and [" + upper + "]");
        }

        @Override
        public String toString() {
            return Long.toString(n);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            Constant other = (Constant) obj;
            return n == other.n;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(n);
        }
    }

    abstract static class Chain extends AbstractLongQueryableExpression {
        protected final LongQueryableExpression next;

        Chain(LongQueryableExpression next) {
            this.next = next;
        }

        @Override
        public Query approximateExists() {
            return next.approximateExists();
        }

        @Override
        public QueryableExpression unknownOp() {
            return next.unknownOp();
        }

        @Override
        public QueryableExpression unknownOp(QueryableExpression rhs) {
            return next.unknownOp(rhs);
        }

        @Override
        protected QueryableExpression add(LongQueryableExpression rhs) {
            return rhs.mapConstant(c -> new Add(this, c));
        }

        @Override
        protected QueryableExpression multiply(LongQueryableExpression rhs) {
            return rhs.mapConstant(c -> new Multiply(this, c));
        }

        @Override
        protected QueryableExpression divide(LongQueryableExpression rhs) {
            return rhs.mapConstant(c -> new Divide(this, c));
        }

        @Override
        public QueryableExpression mapConstant(LongFunction<QueryableExpression> map) {
            return UnqueryableExpression.UNQUERYABLE;
        }
    }
}
