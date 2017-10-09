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

import org.apache.lucene.document.BinaryRange;
import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.BlendedTermQuery;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toSet;

final class QueryAnalyzer {

    private static final Map<Class<? extends Query>, BiFunction<Query, Map<String, Float>, Result>> queryProcessors;

    static {
        Map<Class<? extends Query>, BiFunction<Query, Map<String, Float>, Result>> map = new HashMap<>();
        map.put(MatchNoDocsQuery.class, matchNoDocsQuery());
        map.put(ConstantScoreQuery.class, constantScoreQuery());
        map.put(BoostQuery.class, boostQuery());
        map.put(TermQuery.class, termQuery());
        map.put(TermInSetQuery.class, termInSetQuery());
        map.put(CommonTermsQuery.class, commonTermsQuery());
        map.put(BlendedTermQuery.class, blendedTermQuery());
        map.put(PhraseQuery.class, phraseQuery());
        map.put(MultiPhraseQuery.class, multiPhraseQuery());
        map.put(SpanTermQuery.class, spanTermQuery());
        map.put(SpanNearQuery.class, spanNearQuery());
        map.put(SpanOrQuery.class, spanOrQuery());
        map.put(SpanFirstQuery.class, spanFirstQuery());
        map.put(SpanNotQuery.class, spanNotQuery());
        map.put(BooleanQuery.class, booleanQuery());
        map.put(DisjunctionMaxQuery.class, disjunctionMaxQuery());
        map.put(SynonymQuery.class, synonymQuery());
        map.put(FunctionScoreQuery.class, functionScoreQuery());
        map.put(PointRangeQuery.class, pointRangeQuery());
        map.put(IndexOrDocValuesQuery.class, indexOrDocValuesQuery());
        map.put(ESToParentBlockJoinQuery.class, toParentBlockJoinQuery());
        queryProcessors = Collections.unmodifiableMap(map);
    }

    private QueryAnalyzer() {
    }

    /**
     * Extracts terms and ranges from the provided query. These terms and ranges are stored with the percolator query and
     * used by the percolate query's candidate query as fields to be query by. The candidate query
     * holds the terms from the document to be percolated and allows to the percolate query to ignore
     * percolator queries that we know would otherwise never match.
     *
     * <p>
     * When extracting the terms for the specified query, we can also determine if the percolator query is
     * always going to match. For example if a percolator query just contains a term query or a disjunction
     * query then when the candidate query matches with that, we know the entire percolator query always
     * matches. This allows the percolate query to skip the expensive memory index verification step that
     * it would otherwise have to execute (for example when a percolator query contains a phrase query or a
     * conjunction query).
     *
     * <p>
     * The query analyzer doesn't always extract all terms from the specified query. For example from a
     * boolean query with no should clauses or phrase queries only the longest term are selected,
     * since that those terms are likely to be the rarest. Boolean query's must_not clauses are always ignored.
     *
     * <p>
     * Sometimes the query analyzer can't always extract terms or ranges from a sub query, if that happens then
     * query analysis is stopped and an UnsupportedQueryException is thrown. So that the caller can mark
     * this query in such a way that the PercolatorQuery always verifies if this query with the MemoryIndex.
     */
    static Result analyze(Query query, Map<String, Float> boosts) {
        Class queryClass = query.getClass();
        if (queryClass.isAnonymousClass()) {
            // Sometimes queries have anonymous classes in that case we need the direct super class.
            // (for example blended term query)
            queryClass = queryClass.getSuperclass();
        }
        BiFunction<Query, Map<String, Float>, Result> queryProcessor = queryProcessors.get(queryClass);
        if (queryProcessor != null) {
            return queryProcessor.apply(query, boosts);
        } else {
            throw new UnsupportedQueryException(query);
        }
    }

    private static BiFunction<Query, Map<String, Float>, Result> matchNoDocsQuery() {
        return (query, boosts) -> new Result(true, Collections.emptySet());
    }

    private static BiFunction<Query, Map<String, Float>, Result> constantScoreQuery() {
        return (query, boosts)-> {
            Query wrappedQuery = ((ConstantScoreQuery) query).getQuery();
            return analyze(wrappedQuery, boosts);
        };
    }

    private static BiFunction<Query, Map<String, Float>, Result> boostQuery() {
        return (query, boosts) -> {
            Query wrappedQuery = ((BoostQuery) query).getQuery();
            return analyze(wrappedQuery, boosts);
        };
    }

    private static BiFunction<Query, Map<String, Float>, Result> termQuery() {
        return (query, boosts) -> {
            TermQuery termQuery = (TermQuery) query;
            return new Result(true, Collections.singleton(new QueryExtraction(termQuery.getTerm())));
        };
    }

    private static BiFunction<Query, Map<String, Float>, Result> termInSetQuery() {
        return (query, boosts) -> {
            TermInSetQuery termInSetQuery = (TermInSetQuery) query;
            Set<QueryExtraction> terms = new HashSet<>();
            PrefixCodedTerms.TermIterator iterator = termInSetQuery.getTermData().iterator();
            for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
                terms.add(new QueryExtraction(new Term(iterator.field(), term)));
            }
            return new Result(true, terms);
        };
    }

    private static BiFunction<Query, Map<String, Float>, Result> synonymQuery() {
        return (query, boosts) -> {
            Set<QueryExtraction> terms = ((SynonymQuery) query).getTerms().stream().map(QueryExtraction::new).collect(toSet());
            return new Result(true, terms);
        };
    }

    private static BiFunction<Query, Map<String, Float>, Result> commonTermsQuery() {
        return (query, boosts) -> {
            Set<QueryExtraction> terms = ((CommonTermsQuery) query).getTerms().stream().map(QueryExtraction::new).collect(toSet());
            return new Result(false, terms);
        };
    }

    private static BiFunction<Query, Map<String, Float>, Result> blendedTermQuery() {
        return (query, boosts) -> {
            Set<QueryExtraction> terms = ((BlendedTermQuery) query).getTerms().stream().map(QueryExtraction::new).collect(toSet());
            return new Result(true, terms);
        };
    }

    private static BiFunction<Query, Map<String, Float>, Result> phraseQuery() {
        return (query, boosts) -> {
            Term[] terms = ((PhraseQuery) query).getTerms();
            if (terms.length == 0) {
                return new Result(true, Collections.emptySet());
            }

            // the longest term is likely to be the rarest,
            // so from a performance perspective it makes sense to extract that
            Term longestTerm = terms[0];
            for (Term term : terms) {
                if (longestTerm.bytes().length < term.bytes().length) {
                    longestTerm = term;
                }
            }
            return new Result(false, Collections.singleton(new QueryExtraction(longestTerm)));
        };
    }

    private static BiFunction<Query, Map<String, Float>, Result> multiPhraseQuery() {
        return (query, boosts) -> {
            Term[][] terms = ((MultiPhraseQuery) query).getTermArrays();
            if (terms.length == 0) {
                return new Result(true, Collections.emptySet());
            }

            Set<QueryExtraction> bestTermArr = null;
            for (Term[] termArr : terms) {
                Set<QueryExtraction> queryExtractions = Arrays.stream(termArr).map(QueryExtraction::new).collect(toSet());
                bestTermArr = selectBestExtraction(boosts, bestTermArr, queryExtractions);
            }
            return new Result(false, bestTermArr);
        };
    }

    private static BiFunction<Query, Map<String, Float>, Result> spanTermQuery() {
        return (query, boosts) -> {
            Term term = ((SpanTermQuery) query).getTerm();
            return new Result(true, Collections.singleton(new QueryExtraction(term)));
        };
    }

    private static BiFunction<Query, Map<String, Float>, Result> spanNearQuery() {
        return (query, boosts) -> {
            Set<QueryExtraction> bestClauses = null;
            SpanNearQuery spanNearQuery = (SpanNearQuery) query;
            for (SpanQuery clause : spanNearQuery.getClauses()) {
                Result temp = analyze(clause, boosts);
                bestClauses = selectBestExtraction(boosts, temp.extractions, bestClauses);
            }
            return new Result(false, bestClauses);
        };
    }

    private static BiFunction<Query, Map<String, Float>, Result> spanOrQuery() {
        return (query, boosts) -> {
            Set<QueryExtraction> terms = new HashSet<>();
            SpanOrQuery spanOrQuery = (SpanOrQuery) query;
            for (SpanQuery clause : spanOrQuery.getClauses()) {
                terms.addAll(analyze(clause, boosts).extractions);
            }
            return new Result(false, terms);
        };
    }

    private static BiFunction<Query, Map<String, Float>, Result> spanNotQuery() {
        return (query, boosts) -> {
            Result result = analyze(((SpanNotQuery) query).getInclude(), boosts);
            return new Result(false, result.extractions);
        };
    }

    private static BiFunction<Query, Map<String, Float>, Result> spanFirstQuery() {
        return (query, boosts) -> {
            Result result = analyze(((SpanFirstQuery) query).getMatch(), boosts);
            return new Result(false, result.extractions);
        };
    }

    private static BiFunction<Query, Map<String, Float>, Result> booleanQuery() {
        return (query, boosts) -> {
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
                Set<QueryExtraction> bestClause = null;
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
                        temp = analyze(clause.getQuery(), boosts);
                    } catch (UnsupportedQueryException e) {
                        uqe = e;
                        continue;
                    }
                    bestClause = selectBestExtraction(boosts, temp.extractions, bestClause);
                }
                if (bestClause != null) {
                    return new Result(false, bestClause);
                } else {
                    if (uqe != null) {
                        // we're unable to select the best clause and an exception occurred, so we bail
                        throw uqe;
                    } else {
                        // We didn't find a clause and no exception occurred, so this bq only contained MatchNoDocsQueries,
                        return new Result(true, Collections.emptySet());
                    }
                }
            } else {
                List<Query> disjunctions = new ArrayList<>(numOptionalClauses);
                for (BooleanClause clause : clauses) {
                    if (clause.getOccur() == BooleanClause.Occur.SHOULD) {
                        disjunctions.add(clause.getQuery());
                    }
                }
                return handleDisjunction(disjunctions, minimumShouldMatch, numProhibitedClauses > 0, boosts);
            }
        };
    }

    private static BiFunction<Query, Map<String, Float>, Result> disjunctionMaxQuery() {
        return (query, boosts) -> {
            List<Query> disjuncts = ((DisjunctionMaxQuery) query).getDisjuncts();
            return handleDisjunction(disjuncts, 1, false, boosts);
        };
    }

    private static BiFunction<Query, Map<String, Float>, Result> functionScoreQuery() {
        return (query, boosts) -> {
            FunctionScoreQuery functionScoreQuery = (FunctionScoreQuery) query;
            Result result = analyze(functionScoreQuery.getSubQuery(), boosts);
            // If min_score is specified we can't guarantee upfront that this percolator query matches,
            // so in that case we set verified to false.
            // (if it matches with the percolator document matches with the extracted terms.
            // Min score filters out docs, which is different than the functions, which just influences the score.)
            boolean verified = functionScoreQuery.getMinScore() == null;
            return new Result(verified, result.extractions);
        };
    }

    private static BiFunction<Query, Map<String, Float>, Result> pointRangeQuery() {
        return (query, boosts) -> {
            PointRangeQuery pointRangeQuery = (PointRangeQuery) query;
            if (pointRangeQuery.getNumDims() != 1) {
                throw new UnsupportedQueryException(query);
            }

            byte[] lowerPoint = pointRangeQuery.getLowerPoint();
            byte[] upperPoint = pointRangeQuery.getUpperPoint();

            // Need to check whether upper is not smaller than lower, otherwise NumericUtils.subtract(...) fails IAE
            // If upper is really smaller than lower then we deal with like MatchNoDocsQuery. (verified and no extractions)
            if (new BytesRef(lowerPoint).compareTo(new BytesRef(upperPoint)) > 0) {
                return new Result(true, Collections.emptySet());
            }

            byte[] interval = new byte[16];
            NumericUtils.subtract(16, 0, prepad(upperPoint), prepad(lowerPoint), interval);
            return new Result(false, Collections.singleton(new QueryExtraction(
                new Range(pointRangeQuery.getField(), lowerPoint, upperPoint, interval))
            ));
        };
    }

    private static byte[] prepad(byte[] original) {
        int offset = BinaryRange.BYTES - original.length;
        byte[] result = new byte[BinaryRange.BYTES];
        System.arraycopy(original, 0, result, offset, original.length);
        return result;
    }

    private static BiFunction<Query, Map<String, Float>, Result> indexOrDocValuesQuery() {
        return (query, boosts) -> {
            IndexOrDocValuesQuery indexOrDocValuesQuery = (IndexOrDocValuesQuery) query;
            return analyze(indexOrDocValuesQuery.getIndexQuery(), boosts);
        };
    }

    private static BiFunction<Query, Map<String, Float>, Result> toParentBlockJoinQuery() {
        return (query, boosts) -> {
            ESToParentBlockJoinQuery toParentBlockJoinQuery = (ESToParentBlockJoinQuery) query;
            Result result = analyze(toParentBlockJoinQuery.getChildQuery(), boosts);
            return new Result(false, result.extractions);
        };
    }

    private static Result handleDisjunction(List<Query> disjunctions, int minimumShouldMatch, boolean otherClauses,
                                            Map<String, Float> boosts) {
        boolean verified = minimumShouldMatch <= 1 && otherClauses == false;
        Set<QueryExtraction> terms = new HashSet<>();
        for (Query disjunct : disjunctions) {
            Result subResult = analyze(disjunct, boosts);
            if (subResult.verified == false) {
                verified = false;
            }
            terms.addAll(subResult.extractions);
        }
        return new Result(verified, terms);
    }

    static Set<QueryExtraction> selectBestExtraction(Map<String, Float> boostFields, Set<QueryExtraction> extractions1,
                                                     Set<QueryExtraction> extractions2) {
        assert extractions1 != null || extractions2 != null;
        if (extractions1 == null) {
            return extractions2;
        } else if (extractions2 == null) {
            return extractions1;
        } else {
            Set<QueryExtraction> filtered1;
            Set<QueryExtraction> filtered2;
            if (boostFields.isEmpty() == false) {
                Predicate<QueryExtraction> predicate = extraction -> {
                    String fieldName = extraction.term != null ? extraction.term.field() : extraction.range.fieldName;
                    float boost = boostFields.getOrDefault(fieldName, 1F);
                    return boost != 0F;
                };
                filtered1 = extractions1.stream().filter(predicate).collect(toSet());
                if (filtered1.isEmpty()) {
                    return extractions2;
                }
                filtered2 = extractions2.stream().filter(predicate).collect(toSet());
                if (filtered2.isEmpty()) {
                    return extractions1;
                }

                float extraction1LowestBoost = lowestBoost(filtered1, boostFields);
                float extraction2LowestBoost = lowestBoost(filtered2, boostFields);
                if (extraction1LowestBoost > extraction2LowestBoost) {
                    return extractions1;
                } else if (extraction2LowestBoost > extraction1LowestBoost) {
                    return extractions2;
                }
                // Step out, because boosts are equal, so pick best extraction on either term or range size.
            } else {
                filtered1 = extractions1;
                filtered2 = extractions2;
            }

            // Prefer term based extractions over range based extractions:
            boolean onlyRangeBasedExtractions = true;
            for (QueryExtraction clause : filtered1) {
                if (clause.term != null) {
                    onlyRangeBasedExtractions = false;
                    break;
                }
            }
            for (QueryExtraction clause : filtered2) {
                if (clause.term != null) {
                    onlyRangeBasedExtractions = false;
                    break;
                }
            }

            if (onlyRangeBasedExtractions) {
                BytesRef extraction1SmallestRange = smallestRange(filtered1);
                BytesRef extraction2SmallestRange = smallestRange(filtered2);
                if (extraction1SmallestRange == null) {
                    return extractions2;
                } else if (extraction2SmallestRange == null) {
                    return extractions1;
                }

                // Keep the clause with smallest range, this is likely to be the rarest.
                if (extraction1SmallestRange.compareTo(extraction2SmallestRange) <= 0) {
                    return extractions1;
                } else {
                    return extractions2;
                }
            } else {
                int extraction1ShortestTerm = minTermLength(filtered1);
                int extraction2ShortestTerm = minTermLength(filtered2);
                // keep the clause with longest terms, this likely to be rarest.
                if (extraction1ShortestTerm >= extraction2ShortestTerm) {
                    return extractions1;
                } else {
                    return extractions2;
                }
            }
        }
    }

    private static float lowestBoost(Set<QueryExtraction> extractions, Map<String, Float> boostFields) {
        float lowestBoost = Float.POSITIVE_INFINITY;
        for (QueryExtraction extraction : extractions) {
            String fieldName = extraction.term != null ? extraction.term.field() : extraction.range.fieldName;
            float boost = boostFields.getOrDefault(fieldName, 1F);
            lowestBoost = Math.min(lowestBoost, boost);
        }
        return lowestBoost;
    }

    private static int minTermLength(Set<QueryExtraction> extractions) {
        // In case there are only range extractions, then we return Integer.MIN_VALUE,
        // so that selectBestExtraction(...) we are likely to prefer the extractions that contains at least a single extraction
        if (extractions.stream().filter(queryExtraction -> queryExtraction.term != null).count() == 0 &&
            extractions.stream().filter(queryExtraction -> queryExtraction.range != null).count() > 0) {
            return Integer.MIN_VALUE;
        }

        int min = Integer.MAX_VALUE;
        for (QueryExtraction qt : extractions) {
            if (qt.term != null) {
                min = Math.min(min, qt.bytes().length);
            }
        }
        return min;
    }

    private static BytesRef smallestRange(Set<QueryExtraction> terms) {
        BytesRef min = null;
        for (QueryExtraction qt : terms) {
            if (qt.range != null) {
                if (min == null || qt.range.interval.compareTo(min) < 0) {
                    min = qt.range.interval;
                }
            }
        }
        return min;
    }

    static class Result {

        final Set<QueryExtraction> extractions;
        final boolean verified;

        Result(boolean verified, Set<QueryExtraction> extractions) {
            this.extractions = extractions;
            this.verified = verified;
        }

    }

    static class QueryExtraction {

        final Term term;
        final Range range;

        QueryExtraction(Term term) {
            this.term = term;
            this.range = null;
        }

        QueryExtraction(Range range) {
            this.term = null;
            this.range = range;
        }

        String field() {
            return term != null ? term.field() : null;
        }

        BytesRef bytes() {
            return term != null ? term.bytes() : null;
        }

        String text() {
            return term != null ? term.text() : null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            QueryExtraction queryExtraction = (QueryExtraction) o;
            return Objects.equals(term, queryExtraction.term) &&
                Objects.equals(range, queryExtraction.range);
        }

        @Override
        public int hashCode() {
            return Objects.hash(term, range);
        }

        @Override
        public String toString() {
            return "QueryExtraction{" +
                "term=" + term +
                ",range=" + range +
                '}';
        }
    }

    /**
     * Exception indicating that none or some query terms couldn't extracted from a percolator query.
     */
    static class UnsupportedQueryException extends RuntimeException {

        private final Query unsupportedQuery;

        UnsupportedQueryException(Query unsupportedQuery) {
            super(LoggerMessageFormat.format("no query terms can be extracted from query [{}]", unsupportedQuery));
            this.unsupportedQuery = unsupportedQuery;
        }

        /**
         * The actual Lucene query that was unsupported and caused this exception to be thrown.
         */
        Query getUnsupportedQuery() {
            return unsupportedQuery;
        }
    }

    static class Range {

        final String fieldName;
        final byte[] lowerPoint;
        final byte[] upperPoint;
        final BytesRef interval;

        Range(String fieldName, byte[] lowerPoint, byte[] upperPoint, byte[] interval) {
            this.fieldName = fieldName;
            this.lowerPoint = lowerPoint;
            this.upperPoint = upperPoint;
            // using BytesRef here just to make use of its compareTo method.
            this.interval = new BytesRef(interval);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Range range = (Range) o;
            return Objects.equals(fieldName, range.fieldName) &&
                Arrays.equals(lowerPoint, range.lowerPoint) &&
                Arrays.equals(upperPoint, range.upperPoint);
        }

        @Override
        public int hashCode() {
            int result = 1;
            result += 31 * fieldName.hashCode();
            result += Arrays.hashCode(lowerPoint);
            result += Arrays.hashCode(upperPoint);
            return result;
        }

        @Override
        public String toString() {
            return "Range{" +
                ", fieldName='" + fieldName + '\'' +
                ", interval=" + interval +
                '}';
        }
    }

}
