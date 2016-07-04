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
package org.elasticsearch.percolator;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.HalfFloatPoint;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.BlendedTermQuery;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Utility to extract terms and ranges from queries.
 */
public final class QueryExtractService {

    private static final Map<Class<? extends Query>, Function<Query, Result>> queryProcessors;

    static {
        Map<Class<? extends Query>, Function<Query, Result>> map = new HashMap<>();
        map.put(MatchNoDocsQuery.class, matchNoDocsQuery());
        map.put(ConstantScoreQuery.class, constantScoreQuery());
        map.put(BoostQuery.class, boostQuery());
        map.put(TermQuery.class, termQuery());
        map.put(TermsQuery.class, termsQuery());
        map.put(CommonTermsQuery.class, commonTermsQuery());
        map.put(BlendedTermQuery.class, blendedTermQuery());
        map.put(PhraseQuery.class, phraseQuery());
        map.put(SpanTermQuery.class, spanTermQuery());
        map.put(SpanNearQuery.class, spanNearQuery());
        map.put(SpanOrQuery.class, spanOrQuery());
        map.put(SpanFirstQuery.class, spanFirstQuery());
        map.put(SpanNotQuery.class, spanNotQuery());
        map.put(BooleanQuery.class, booleanQuery());
        map.put(DisjunctionMaxQuery.class, disjunctionMaxQuery());
        map.put(SynonymQuery.class, synonymQuery());
        map.put(PointRangeQuery.class, pointRangeQuery());
        queryProcessors = Collections.unmodifiableMap(map);
    }

    private QueryExtractService() {
    }

    /**
     * Extracts all terms and ranges from the provided query.
     * <p>
     * From boolean query with no should clauses or phrase queries only the longest term are selected,
     * since that those terms are likely to be the rarest. Boolean query's must_not clauses are always ignored.
     * <p>
     * If from part of the query, no query terms can be extracted then term extraction is stopped and
     * an UnsupportedQueryException is thrown.
     */
    public static Result extractQueryTerms(Query query) {
        Class queryClass = query.getClass();
        if (queryClass.isAnonymousClass()) {
            // Sometimes queries have anonymous classes in that case we need the direct super class.
            // (for example blended term query)
            queryClass = queryClass.getSuperclass();
        }
        Function<Query, Result> queryProcessor = queryProcessors.get(queryClass);
        if (queryProcessor != null) {
            return queryProcessor.apply(query);
        } else {
            throw new UnsupportedQueryException(query);
        }
    }

    static Function<Query, Result> matchNoDocsQuery() {
        return (query -> new Result(true, Collections.emptySet(), Collections.emptySet()));
    }

    static Function<Query, Result> constantScoreQuery() {
        return query -> {
            Query wrappedQuery = ((ConstantScoreQuery) query).getQuery();
            return extractQueryTerms(wrappedQuery);
        };
    }

    static Function<Query, Result> boostQuery() {
        return query -> {
            Query wrappedQuery = ((BoostQuery) query).getQuery();
            return extractQueryTerms(wrappedQuery);
        };
    }

    static Function<Query, Result> termQuery() {
        return (query -> {
            TermQuery termQuery = (TermQuery) query;
            return new Result(true, Collections.singleton(termQuery.getTerm()), Collections.emptySet());
        });
    }

    static Function<Query, Result> termsQuery() {
        return query -> {
            TermsQuery termsQuery = (TermsQuery) query;
            Set<Term> terms = new HashSet<>();
            PrefixCodedTerms.TermIterator iterator = termsQuery.getTermData().iterator();
            for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
                terms.add(new Term(iterator.field(), term));
            }
            return new Result(true, terms, Collections.emptySet());
        };
    }

    static Function<Query, Result> synonymQuery() {
        return query -> {
            Set<Term> terms = new HashSet<>(((SynonymQuery) query).getTerms());
            return new Result(true, terms, Collections.emptySet());
        };
    }

    static Function<Query, Result> commonTermsQuery() {
        return query -> {
            List<Term> terms = ((CommonTermsQuery) query).getTerms();
            return new Result(false, new HashSet<>(terms), Collections.emptySet());
        };
    }

    static Function<Query, Result> blendedTermQuery() {
        return query -> {
            List<Term> terms = ((BlendedTermQuery) query).getTerms();
            return new Result(true, new HashSet<>(terms), Collections.emptySet());
        };
    }

    static Function<Query, Result> phraseQuery() {
        return query -> {
            Term[] terms = ((PhraseQuery) query).getTerms();
            if (terms.length == 0) {
                return new Result(true, Collections.emptySet(), Collections.emptySet());
            }

            // the longest term is likely to be the rarest,
            // so from a performance perspective it makes sense to extract that
            Term longestTerm = terms[0];
            for (Term term : terms) {
                if (longestTerm.bytes().length < term.bytes().length) {
                    longestTerm = term;
                }
            }
            return new Result(false, Collections.singleton(longestTerm), Collections.emptySet());
        };
    }

    static Function<Query, Result> pointRangeQuery() {
        return query -> {
            PointRangeQuery pointRangeQuery = (PointRangeQuery) query;
            // Is this too hacky? Otherwise we need to use the MapperService to determine what kind of number field this is
            Class<?> enclosingClass = pointRangeQuery.getClass().getEnclosingClass();
            // We need to check the number type before indexing to ensure we support it.
            RangeType rangeType = null;
            if (LongPoint.class.isAssignableFrom(enclosingClass)) {
                rangeType = RangeType.LONG;
            } else if (IntPoint.class.isAssignableFrom(enclosingClass)) {
                rangeType = RangeType.INT;
            } else if (DoublePoint.class.isAssignableFrom(enclosingClass)) {
                rangeType = RangeType.DOUBLE;
            } else if (FloatPoint.class.isAssignableFrom(enclosingClass)) {
                rangeType = RangeType.FLOAT;
            } else if (HalfFloatPoint.class.isAssignableFrom(enclosingClass)) {
                rangeType = RangeType.HALF_FLOAT;
            } else if (InetAddressPoint.class.isAssignableFrom(enclosingClass)) {
                rangeType = RangeType.IP;
            }
            if (rangeType != null) {
                return new Result(false, Collections.emptySet(), Collections.singleton(new Range(rangeType, pointRangeQuery)));
            } else {
                throw new UnsupportedQueryException(pointRangeQuery);
            }
        };
    }

    static Function<Query, Result> spanTermQuery() {
        return query -> {
            Term term = ((SpanTermQuery) query).getTerm();
            return new Result(true, Collections.singleton(term), Collections.emptySet());
        };
    }

    static Function<Query, Result> spanNearQuery() {
        return query -> {
            SpanNearQuery spanNearQuery = (SpanNearQuery) query;
            if (spanNearQuery.getClauses().length == 0) {
                return new Result(true, Collections.emptySet(), Collections.emptySet());
            }

            Result bestClauses = null;
            for (SpanQuery clause : spanNearQuery.getClauses()) {
                Result temp = extractQueryTerms(clause);
                bestClauses = selectBestResult(temp, bestClauses);
            }
            return new Result(false, bestClauses.terms, bestClauses.ranges);
        };
    }

    static Function<Query, Result> spanOrQuery() {
        return query -> {
            Set<Term> terms = new HashSet<>();
            SpanOrQuery spanOrQuery = (SpanOrQuery) query;
            for (SpanQuery clause : spanOrQuery.getClauses()) {
                terms.addAll(extractQueryTerms(clause).terms);
            }
            return new Result(false, terms, Collections.emptySet());
        };
    }

    static Function<Query, Result> spanNotQuery() {
        return query -> {
            Result result = extractQueryTerms(((SpanNotQuery) query).getInclude());
            return new Result(false, result.terms, Collections.emptySet());
        };
    }

    static Function<Query, Result> spanFirstQuery() {
        return query -> {
            Result result = extractQueryTerms(((SpanFirstQuery) query).getMatch());
            return new Result(false, result.terms, Collections.emptySet());
        };
    }

    static Function<Query, Result> booleanQuery() {
        return query -> {
            BooleanQuery bq = (BooleanQuery) query;
            List<BooleanClause> clauses = bq.clauses();
            int minimumShouldMatch = bq.getMinimumNumberShouldMatch();
            int numRequiredClauses = 0;
            int numOptionalClauses = 0;
            int numProhibitedClauses = 0;
            for (BooleanClause clause : clauses) {
                if (clause.isRequired()) {
                    numRequiredClauses++;
                }
                if (clause.isProhibited()) {
                    numProhibitedClauses++;
                }
                if (clause.getOccur() == BooleanClause.Occur.SHOULD) {
                    numOptionalClauses++;
                }
            }
            if (numRequiredClauses > 0) {
                Result bestClause = null;
                UnsupportedQueryException uqe = null;
                for (BooleanClause clause : clauses) {
                    if (clause.isRequired() == false) {
                        // skip must_not clauses, we don't need to remember the things that do *not* match...
                        // skip should clauses, this bq has must clauses, so we don't need to remember should clauses,
                        // since they are completely optional.
                        continue;
                    }

                    Result temp;
                    try {
                        temp = extractQueryTerms(clause.getQuery());
                    } catch (UnsupportedQueryException e) {
                        uqe = e;
                        continue;
                    }
                    bestClause = selectBestResult(temp, bestClause);
                }
                if (bestClause != null) {
                    return new Result(false, bestClause.terms, bestClause.ranges);
                } else {
                    if (uqe != null) {
                        // we're unable to select the best clause and an exception occurred, so we bail
                        throw uqe;
                    } else {
                        // We didn't find a clause and no exception occurred, so this bq only contained MatchNoDocsQueries,
                        return new Result(true, Collections.emptySet(), Collections.emptySet());
                    }
                }
            } else {
                List<Query> disjunctions = new ArrayList<>(numOptionalClauses);
                for (BooleanClause clause : clauses) {
                    if (clause.getOccur() == BooleanClause.Occur.SHOULD) {
                        disjunctions.add(clause.getQuery());
                    }
                }
                return handleDisjunction(disjunctions, minimumShouldMatch, numProhibitedClauses > 0);
            }
        };
    }

    static Function<Query, Result> disjunctionMaxQuery() {
        return query -> {
            List<Query> disjuncts = ((DisjunctionMaxQuery) query).getDisjuncts();
            return handleDisjunction(disjuncts, 1, false);
        };
    }

    static Result handleDisjunction(List<Query> disjunctions, int minimumShouldMatch, boolean otherClauses) {
        boolean verified = minimumShouldMatch <= 1 && otherClauses == false;
        Set<Term> terms = new HashSet<>();
        Set<Range> ranges = new HashSet<>();
        for (Query disjunct : disjunctions) {
            Result subResult = extractQueryTerms(disjunct);
            if (subResult.verified == false) {
                verified = false;
            }
            terms.addAll(subResult.terms);
            ranges.addAll(subResult.ranges);
        }
        return new Result(verified, terms, ranges);
    }

    static Result selectBestResult(Result result1, Result result2) {
        if (result1 == null) {
            return result2;
        } else if (result2 == null) {
            return result1;
        } else {
            boolean onlyTerms = result1.ranges.isEmpty() && result2.ranges.isEmpty();
            if (onlyTerms) {
                int terms1ShortestTerm = minTermLength(result1.terms);
                int terms2ShortestTerm = minTermLength(result2.terms);
                // keep the clause with longest terms, this likely to be rarest.
                if (terms1ShortestTerm >= terms2ShortestTerm) {
                    return result1;
                } else {
                    return result2;
                }
            } else {
                // 'picking the longest terms' heuristic does't work with numbers, so just pick the result with the least clauses:
                int clauseSize1 = result1.ranges.size() + result1.terms.size();
                int clauseSize2 = result2.ranges.size() + result2.terms.size();
                if (clauseSize1 >= clauseSize2) {
                    return result1;
                } else {
                    return result2;
                }
            }
        }
    }

    static int minTermLength(Set<Term> terms) {
        int min = Integer.MAX_VALUE;
        for (Term term : terms) {
            min = Math.min(min, term.bytes().length);
        }
        return min;
    }

    static class Result {

        final Set<Term> terms;
        final boolean verified;
        final Set<Range> ranges;

        Result(boolean verified, Set<Term> terms, Set<Range> ranges) {
            this.terms = terms;
            this.verified = verified;
            this.ranges = ranges;
        }

    }

    static class Range {

        final RangeType rangeType;
        final String fieldName;
        final byte[] lowerPoint;
        final byte[] upperPoint;

        Range(RangeType rangeType, PointRangeQuery query) {
            this.rangeType = rangeType;
            this.fieldName = query.getField();
            this.lowerPoint = query.getLowerPoint();
            this.upperPoint = query.getUpperPoint();
        }
    }

    enum RangeType {

        LONG {

            @Override
            Object decodePackedValue(byte[] packedValue) {
                return LongPoint.decodeDimension(packedValue, 0);
            }

            @Override
            Field createField(String fieldName, byte[] packedValue) {
                long value = LongPoint.decodeDimension(packedValue, 0);
                return new LongPoint(fieldName, value);
            }
        },
        INT {

            @Override
            Object decodePackedValue(byte[] packedValue) {
                return IntPoint.decodeDimension(packedValue, 0);
            }

            @Override
            Field createField(String fieldName, byte[] packedValue) {
                int value =  IntPoint.decodeDimension(packedValue, 0);
                return new IntPoint(fieldName, value);
            }
        },
        DOUBLE {

            @Override
            Object decodePackedValue(byte[] packedValue) {
                return DoublePoint.decodeDimension(packedValue, 0);
            }

            @Override
            Field createField(String fieldName, byte[] packedValue) {
                double value = DoublePoint.decodeDimension(packedValue, 0);
                return new DoublePoint(fieldName, value);
            }
        },
        FLOAT {

            @Override
            Object decodePackedValue(byte[] packedValue) {
                return FloatPoint.decodeDimension(packedValue, 0);
            }

            @Override
            Field createField(String fieldName, byte[] packedValue) {
                float value = FloatPoint.decodeDimension(packedValue, 0);
                return new FloatPoint(fieldName, value);
            }
        },
        HALF_FLOAT {

            @Override
            Object decodePackedValue(byte[] packedValue) {
                return HalfFloatPoint.decodeDimension(packedValue, 0);
            }

            @Override
            Field createField(String fieldName, byte[] packedValue) {
                float value = HalfFloatPoint.decodeDimension(packedValue, 0);
                return new HalfFloatPoint(fieldName, value);
            }
        },
        IP {

            @Override
            Object decodePackedValue(byte[] packedValue) {
                return InetAddressPoint.decode(packedValue);
            }

            @Override
            Field createField(String fieldName, byte[] packedValue) {
                InetAddress value  = InetAddressPoint.decode(packedValue);
                return new InetAddressPoint(fieldName, value);
            }
        };

        abstract Object decodePackedValue(byte[] packedValue);

        abstract Field createField(String fieldName, byte[] packedValue);

    }

    /**
     * Exception indicating that none or some query terms couldn't extracted from a percolator query.
     */
    static class UnsupportedQueryException extends RuntimeException {

        private final Query unsupportedQuery;

        public UnsupportedQueryException(Query unsupportedQuery) {
            super(LoggerMessageFormat.format("no query terms can be extracted from query [{}]", unsupportedQuery));
            this.unsupportedQuery = unsupportedQuery;
        }

        /**
         * The actual Lucene query that was unsupported and caused this exception to be thrown.
         */
        public Query getUnsupportedQuery() {
            return unsupportedQuery;
        }
    }

}
