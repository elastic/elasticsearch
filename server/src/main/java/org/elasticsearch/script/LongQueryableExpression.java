/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.QueryableExpression.Chain;

import java.util.function.LongFunction;

/**
 * Approximates the reversal of two's complement {@code long} operations
 * into Lucene {@link Query queries}.
 */
public class LongQueryableExpression {
    public abstract static class Field extends QueryableExpression {
        @Override
        protected final QueryableExpression asConstantLong(LongFunction<QueryableExpression> map) {
            return UNQUERYABLE;
        }

        @Override
        public QueryableExpression add(QueryableExpression rhs) {
            return rhs.asConstantLong(c -> c == 0 ? this : new Add(this, c));
        }

        @Override
        public QueryableExpression multiply(QueryableExpression rhs) {
            return rhs.asConstantLong(c -> {
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
        public QueryableExpression divide(QueryableExpression rhs) {
            return rhs.asConstantLong(c -> {
                if (c == -1L) {
                    return new Multiply(this, -1L);
                }
                if (c == 0L) {
                    // The script is likely to return NaN, but we can't query for that.
                    return UNQUERYABLE;
                }
                if (c == 1L) {
                    return this;
                }
                return new Divide(this, c);
            });
        }
    }

    /**
     * Add a constant to an expression or subtract a constant from an
     * expression. This can precisely reverse two's complement addition.
     */
    static class Add extends Chain {
        private final long n;

        private Add(QueryableExpression next, long n) {
            super(next);
            if (n == 0) {
                throw new IllegalArgumentException("Don't create Add nodes for identity");
            }
            this.n = n;
        }

        @Override
        public QueryableExpression add(QueryableExpression rhs) {
            return rhs.asConstantLong(c -> new Add(next, n + c));
        }

        @Override
        public QueryableExpression multiply(QueryableExpression rhs) {
            // TODO combine?
            return UNQUERYABLE;
        }

        @Override
        public QueryableExpression divide(QueryableExpression rhs) {
            // TODO combine?
            return UNQUERYABLE;
        }

        @Override
        public Query termQuery(long term, SearchExecutionContext context) {
            return next.termQuery(term - n, context);
        }

        @Override
        public Query rangeQuery(long lower, long upper, SearchExecutionContext context) {
            lower -= n;
            upper -= n;
            if (lower < upper) {
                return next.rangeQuery(lower, upper, context);
            }
            BooleanQuery.Builder bool = new BooleanQuery.Builder();
            bool.add(next.rangeQuery(Long.MIN_VALUE, upper, context), Occur.SHOULD);
            bool.add(next.rangeQuery(lower, Long.MAX_VALUE, context), Occur.SHOULD);
            return bool.build();
        }
    }

    /**
     * Multiply an expression by a constant. Instead of trying to accurately
     * reverse overflows this matches all of the values that cause the
     * multiplication to overflow.
     */
    static class Multiply extends Chain {
        private final long n;

        private Multiply(QueryableExpression next, long n) {
            super(next);
            if (n == 0) {
                throw new IllegalArgumentException("Don't create Multiply nodes 0");
            }
            if (n == 1) {
                throw new IllegalArgumentException("Don't create Multiply nodes for identity");
            }
            this.n = n;
        }

        @Override
        public QueryableExpression add(QueryableExpression rhs) {
            // TODO chain these properly
            return UNQUERYABLE;
        }

        @Override
        public QueryableExpression multiply(QueryableExpression rhs) {
            // TODO combine?
            return UNQUERYABLE;
        }

        @Override
        public QueryableExpression divide(QueryableExpression rhs) {
            // TODO combine?
            return UNQUERYABLE;
        }

        @Override
        public Query termQuery(long term, SearchExecutionContext context) {
            return withOverflow(next.termQuery(term / n, context), context);
        }

        @Override
        public Query rangeQuery(long lower, long upper, SearchExecutionContext context) {
            long scaledLower = lower / n;
            long scaledUpper = upper / n;
            return withOverflow(next.rangeQuery(Math.min(scaledLower, scaledUpper), Math.max(scaledLower, scaledUpper), context), context);
        }

        /**
         * Returns a query that matches the provided query or any overflows.
         * <p>
         * The current implementation just ORs the query with everything that
         * would overflow the long. It works, but its inefficient if they overlap.
         */
        private Query withOverflow(Query query, SearchExecutionContext context) {
            BooleanQuery.Builder bool = new BooleanQuery.Builder();
            bool.add(query, Occur.SHOULD);
            long overflowFromMin = Long.MIN_VALUE / n;
            long overflowFromMax = Long.MAX_VALUE / n;
            bool.add(next.rangeQuery(Math.max(overflowFromMin, overflowFromMax), Long.MAX_VALUE, context), Occur.SHOULD);
            bool.add(next.rangeQuery(Long.MIN_VALUE, Math.min(overflowFromMin, overflowFromMax), context), Occur.SHOULD);
            return bool.build();
        }
    }

    /**
     * Divide an expression by a constant. This can accurately reverse the
     * division into range queries.
     */
    static class Divide extends Chain {
        private final long n;

        private Divide(QueryableExpression next, long n) {
            super(next);
            if (n == 0) {
                throw new IllegalArgumentException("Don't create Divide nodes for 0");
            }
            if (n == 1) {
                throw new IllegalArgumentException("Don't create Divide nodes for identity");
            }
            if (n == -1) {
                throw new IllegalArgumentException("Use Mutliply for division by -1");
            }
            this.n = n;
        }

        @Override
        public QueryableExpression add(QueryableExpression rhs) {
            // TODO combine?
            return UNQUERYABLE;
        }

        @Override
        public QueryableExpression multiply(QueryableExpression rhs) {
            // TODO combine?
            return UNQUERYABLE;
        }

        @Override
        public QueryableExpression divide(QueryableExpression rhs) {
            // TODO combine?
            return UNQUERYABLE;
        }

        @Override
        public Query termQuery(long term, SearchExecutionContext context) {
            /*
             * Division rounds to 0. So let's get 0 out of the way because
             * everything rounds towards it.
             */
            if (term == 0) {
                if (n < 0) {
                    return next.rangeQuery(n + 1, -(n + 1), context);
                }
                return next.rangeQuery(-(n - 1), n - 1, context);
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
                return next.rangeQuery(min, precise, context);
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
            return next.rangeQuery(precise, max, context);
        }

        @Override
        public Query rangeQuery(long lower, long upper, SearchExecutionContext context) {
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
            return next.rangeQuery(min, max, context);
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
    }

    static class Constant extends QueryableExpression {
        private final long n;

        Constant(long n) {
            this.n = n;
        }

        @Override
        public QueryableExpression add(QueryableExpression rhs) {
            QueryableExpression asConstant = rhs.asConstantLong(c -> new Constant(n + c));
            if (asConstant != UNQUERYABLE) {
                return asConstant;
            }
            return rhs.add(this);
        }

        @Override
        public QueryableExpression multiply(QueryableExpression rhs) {
            QueryableExpression asConstant = rhs.asConstantLong(c -> new Constant(n * c));
            if (asConstant != UNQUERYABLE) {
                return asConstant;
            }
            return rhs.multiply(this);
        }

        @Override
        public QueryableExpression divide(QueryableExpression rhs) {
            QueryableExpression asConstant = rhs.asConstantLong(c -> new Constant(n / c));
            if (asConstant != UNQUERYABLE) {
                return asConstant;
            }
            return rhs.divide(this);
        }

        @Override
        protected QueryableExpression asConstantLong(LongFunction<QueryableExpression> map) {
            return map.apply(n);
        }

        @Override
        public Query termQuery(long term, SearchExecutionContext context) {
            if (n == term) {
                return new MatchAllDocsQuery();
            }
            return new MatchNoDocsQuery("[" + term + "] != [" + n + "]");
        }

        @Override
        public Query rangeQuery(long lower, long upper, SearchExecutionContext context) {
            if (lower <= n && n <= upper) {
                return new MatchAllDocsQuery();
            }
            return new MatchNoDocsQuery("value [" + n + "] is not between [" + lower + "] and [" + upper + "]");
        }
    }
}
