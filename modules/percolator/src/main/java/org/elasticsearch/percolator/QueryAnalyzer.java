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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
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
import org.elasticsearch.Version;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.util.Map.entry;
import static java.util.stream.Collectors.toSet;

final class QueryAnalyzer {

    private static final Map<Class<? extends Query>, BiFunction<Query, Version, Result>> QUERY_PROCESSORS = Map.ofEntries(
        entry(MatchNoDocsQuery.class, matchNoDocsQuery()),
        entry(MatchAllDocsQuery.class, matchAllDocsQuery()),
        entry(ConstantScoreQuery.class, constantScoreQuery()),
        entry(BoostQuery.class, boostQuery()),
        entry(TermQuery.class, termQuery()),
        entry(TermInSetQuery.class, termInSetQuery()),
        entry(BlendedTermQuery.class, blendedTermQuery()),
        entry(PhraseQuery.class, phraseQuery()),
        entry(MultiPhraseQuery.class, multiPhraseQuery()),
        entry(SpanTermQuery.class, spanTermQuery()),
        entry(SpanNearQuery.class, spanNearQuery()),
        entry(SpanOrQuery.class, spanOrQuery()),
        entry(SpanFirstQuery.class, spanFirstQuery()),
        entry(SpanNotQuery.class, spanNotQuery()),
        entry(BooleanQuery.class, booleanQuery()),
        entry(DisjunctionMaxQuery.class, disjunctionMaxQuery()),
        entry(SynonymQuery.class, synonymQuery()),
        entry(FunctionScoreQuery.class, functionScoreQuery()),
        entry(PointRangeQuery.class, pointRangeQuery()),
        entry(IndexOrDocValuesQuery.class, indexOrDocValuesQuery()),
        entry(ESToParentBlockJoinQuery.class, toParentBlockJoinQuery()));

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
     *
     * @param query         The query to analyze.
     * @param indexVersion  The create version of the index containing the percolator queries.
     */
    static Result analyze(Query query, Version indexVersion) {
        Class<?> queryClass = query.getClass();
        if (queryClass.isAnonymousClass()) {
            // sometimes queries have anonymous classes in that case we need the direct super class (e.g., blended term query)
            queryClass = queryClass.getSuperclass();
        }
        assert Query.class.isAssignableFrom(queryClass) : query.getClass();
        BiFunction<Query, Version, Result> queryProcessor = QUERY_PROCESSORS.get(queryClass);
        if (queryProcessor != null) {
            return queryProcessor.apply(query, indexVersion);
        } else {
            throw new UnsupportedQueryException(query);
        }
    }

    private static BiFunction<Query, Version, Result> matchNoDocsQuery() {
        return (query, version) -> new Result(true, Collections.emptySet(), 0);
    }

    private static BiFunction<Query, Version, Result> matchAllDocsQuery() {
        return (query, version) -> new Result(true, true);
    }

    private static BiFunction<Query, Version, Result> constantScoreQuery() {
        return (query, boosts) -> {
            Query wrappedQuery = ((ConstantScoreQuery) query).getQuery();
            return analyze(wrappedQuery, boosts);
        };
    }

    private static BiFunction<Query, Version, Result> boostQuery() {
        return (query, version) -> {
            Query wrappedQuery = ((BoostQuery) query).getQuery();
            return analyze(wrappedQuery, version);
        };
    }

    private static BiFunction<Query, Version, Result> termQuery() {
        return (query, version) -> {
            TermQuery termQuery = (TermQuery) query;
            return new Result(true, Collections.singleton(new QueryExtraction(termQuery.getTerm())), 1);
        };
    }

    private static BiFunction<Query, Version, Result> termInSetQuery() {
        return (query, version) -> {
            TermInSetQuery termInSetQuery = (TermInSetQuery) query;
            Set<QueryExtraction> terms = new HashSet<>();
            PrefixCodedTerms.TermIterator iterator = termInSetQuery.getTermData().iterator();
            for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
                terms.add(new QueryExtraction(new Term(iterator.field(), term)));
            }
            return new Result(true, terms, Math.min(1, terms.size()));
        };
    }

    private static BiFunction<Query, Version, Result> synonymQuery() {
        return (query, version) -> {
            Set<QueryExtraction> terms = ((SynonymQuery) query).getTerms().stream().map(QueryExtraction::new).collect(toSet());
            return new Result(true, terms, Math.min(1, terms.size()));
        };
    }

    private static BiFunction<Query, Version, Result> blendedTermQuery() {
        return (query, version) -> {
            Set<QueryExtraction> terms = ((BlendedTermQuery) query).getTerms().stream().map(QueryExtraction::new).collect(toSet());
            return new Result(true, terms, Math.min(1, terms.size()));
        };
    }

    private static BiFunction<Query, Version, Result> phraseQuery() {
        return (query, version) -> {
            Term[] terms = ((PhraseQuery) query).getTerms();
            if (terms.length == 0) {
                return new Result(true, Collections.emptySet(), 0);
            }

            Set<QueryExtraction> extractions = Arrays.stream(terms).map(QueryExtraction::new).collect(toSet());
            return new Result(false, extractions, extractions.size());
        };
    }

    private static BiFunction<Query, Version, Result> multiPhraseQuery() {
        return (query, version) -> {
            Term[][] terms = ((MultiPhraseQuery) query).getTermArrays();
            if (terms.length == 0) {
                return new Result(true, Collections.emptySet(), 0);
            }

            // This query has the same problem as boolean queries when it comes to duplicated terms
            // So to keep things simple, we just rewrite to a boolean query
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (Term[] termArr : terms) {
                BooleanQuery.Builder subBuilder = new BooleanQuery.Builder();
                for (Term term : termArr) {
                    subBuilder.add(new TermQuery(term), Occur.SHOULD);
                }
                builder.add(subBuilder.build(), Occur.FILTER);
            }
            // Make sure to unverify the result
            return booleanQuery().apply(builder.build(), version).unverify();
        };
    }

    private static BiFunction<Query, Version, Result> spanTermQuery() {
        return (query, version) -> {
            Term term = ((SpanTermQuery) query).getTerm();
            return new Result(true, Collections.singleton(new QueryExtraction(term)), 1);
        };
    }

    private static BiFunction<Query, Version, Result> spanNearQuery() {
        return (query, version) -> {
            SpanNearQuery spanNearQuery = (SpanNearQuery) query;
            // This has the same problem as boolean queries when it comes to duplicated clauses
            // so we rewrite to a boolean query to keep things simple.
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (SpanQuery clause : spanNearQuery.getClauses()) {
                builder.add(clause, Occur.FILTER);
            }
            // make sure to unverify the result
            return booleanQuery().apply(builder.build(), version).unverify();
        };
    }

    private static BiFunction<Query, Version, Result> spanOrQuery() {
        return (query, version) -> {
            SpanOrQuery spanOrQuery = (SpanOrQuery) query;
            // handle it like a boolean query to not dulplicate eg. logic
            // about duplicated terms
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (SpanQuery clause : spanOrQuery.getClauses()) {
                builder.add(clause, Occur.SHOULD);
            }
            return booleanQuery().apply(builder.build(), version);
        };
    }

    private static BiFunction<Query, Version, Result> spanNotQuery() {
        return (query, version) -> {
            Result result = analyze(((SpanNotQuery) query).getInclude(), version);
            return new Result(false, result.extractions, result.minimumShouldMatch);
        };
    }

    private static BiFunction<Query, Version, Result> spanFirstQuery() {
        return (query, version) -> {
            Result result = analyze(((SpanFirstQuery) query).getMatch(), version);
            return new Result(false, result.extractions, result.minimumShouldMatch);
        };
    }

    private static BiFunction<Query, Version, Result> booleanQuery() {
        return (query, version) -> {
            BooleanQuery bq = (BooleanQuery) query;
            int minimumShouldMatch = bq.getMinimumNumberShouldMatch();
            List<Query> requiredClauses = new ArrayList<>();
            List<Query> optionalClauses = new ArrayList<>();
            boolean hasProhibitedClauses = false;
            for (BooleanClause clause : bq.clauses()) {
                if (clause.isRequired()) {
                    requiredClauses.add(clause.getQuery());
                } else if (clause.isProhibited()) {
                    hasProhibitedClauses = true;
                } else {
                    assert clause.getOccur() == Occur.SHOULD;
                    optionalClauses.add(clause.getQuery());
                }
            }

            if (minimumShouldMatch > optionalClauses.size()
                    || (requiredClauses.isEmpty() && optionalClauses.isEmpty())) {
                return new Result(false, Collections.emptySet(), 0);
            }

            if (requiredClauses.size() > 0) {
                if (minimumShouldMatch > 0) {
                    // mix of required clauses and required optional clauses, we turn it into
                    // a pure conjunction by moving the optional clauses to a sub query to
                    // simplify logic
                    BooleanQuery.Builder minShouldMatchQuery = new BooleanQuery.Builder();
                    minShouldMatchQuery.setMinimumNumberShouldMatch(minimumShouldMatch);
                    for (Query q : optionalClauses) {
                        minShouldMatchQuery.add(q, Occur.SHOULD);
                    }
                    requiredClauses.add(minShouldMatchQuery.build());
                    optionalClauses.clear();
                    minimumShouldMatch = 0;
                } else {
                    optionalClauses.clear(); // only matter for scoring, not matching
                }
            }

            // Now we now have either a pure conjunction or a pure disjunction, with at least one clause
            Result result;
            if (requiredClauses.size() > 0) {
                assert optionalClauses.isEmpty();
                assert minimumShouldMatch == 0;
                result = handleConjunctionQuery(requiredClauses, version);
            } else {
                assert requiredClauses.isEmpty();
                if (minimumShouldMatch == 0) {
                    // Lucene always requires one matching clause for disjunctions
                    minimumShouldMatch = 1;
                }
                result = handleDisjunctionQuery(optionalClauses, minimumShouldMatch, version);
            }

            if (hasProhibitedClauses) {
                result = result.unverify();
            }

            return result;
        };
    }

    private static BiFunction<Query, Version, Result> disjunctionMaxQuery() {
        return (query, version) -> {
            List<Query> disjuncts = ((DisjunctionMaxQuery) query).getDisjuncts();
            if (disjuncts.isEmpty()) {
                return new Result(false, Collections.emptySet(), 0);
            } else {
                return handleDisjunctionQuery(disjuncts, 1, version);
            }
        };
    }

    private static BiFunction<Query, Version, Result> functionScoreQuery() {
        return (query, version) -> {
            FunctionScoreQuery functionScoreQuery = (FunctionScoreQuery) query;
            Result result = analyze(functionScoreQuery.getSubQuery(), version);

            // If min_score is specified we can't guarantee upfront that this percolator query matches,
            // so in that case we set verified to false.
            // (if it matches with the percolator document matches with the extracted terms.
            // Min score filters out docs, which is different than the functions, which just influences the score.)
            boolean verified = result.verified && functionScoreQuery.getMinScore() == null;
            if (result.matchAllDocs) {
                return new Result(result.matchAllDocs, verified);
            } else {
                return new Result(verified, result.extractions, result.minimumShouldMatch);
            }
        };
    }

    private static BiFunction<Query, Version, Result> pointRangeQuery() {
        return (query, version) -> {
            PointRangeQuery pointRangeQuery = (PointRangeQuery) query;
            if (pointRangeQuery.getNumDims() != 1) {
                throw new UnsupportedQueryException(query);
            }

            byte[] lowerPoint = pointRangeQuery.getLowerPoint();
            byte[] upperPoint = pointRangeQuery.getUpperPoint();

            // Need to check whether upper is not smaller than lower, otherwise NumericUtils.subtract(...) fails IAE
            // If upper is really smaller than lower then we deal with like MatchNoDocsQuery. (verified and no extractions)
            if (new BytesRef(lowerPoint).compareTo(new BytesRef(upperPoint)) > 0) {
                return new Result(true, Collections.emptySet(), 0);
            }

            byte[] interval = new byte[16];
            NumericUtils.subtract(16, 0, prepad(upperPoint), prepad(lowerPoint), interval);
            return new Result(false, Collections.singleton(new QueryExtraction(
                    new Range(pointRangeQuery.getField(), lowerPoint, upperPoint, interval))), 1);
        };
    }

    private static byte[] prepad(byte[] original) {
        int offset = BinaryRange.BYTES - original.length;
        byte[] result = new byte[BinaryRange.BYTES];
        System.arraycopy(original, 0, result, offset, original.length);
        return result;
    }

    private static BiFunction<Query, Version, Result> indexOrDocValuesQuery() {
        return (query, version) -> {
            IndexOrDocValuesQuery indexOrDocValuesQuery = (IndexOrDocValuesQuery) query;
            return analyze(indexOrDocValuesQuery.getIndexQuery(), version);
        };
    }

    private static BiFunction<Query, Version, Result> toParentBlockJoinQuery() {
        return (query, version) -> {
            ESToParentBlockJoinQuery toParentBlockJoinQuery = (ESToParentBlockJoinQuery) query;
            Result result = analyze(toParentBlockJoinQuery.getChildQuery(), version);
            return new Result(false, result.extractions, result.minimumShouldMatch);
        };
    }

    private static Result handleConjunctionQuery(List<Query> conjunctions, Version version) {
        UnsupportedQueryException uqe = null;
        List<Result> results = new ArrayList<>(conjunctions.size());
        boolean success = false;
        for (Query query : conjunctions) {
            try {
                Result subResult = analyze(query, version);
                if (subResult.isMatchNoDocs()) {
                    return subResult;
                }
                results.add(subResult);
                success = true;
            } catch (UnsupportedQueryException e) {
                uqe = e;
            }
        }

                    if (success == false) {
            // No clauses could be extracted
                        if (uqe != null) {

                            throw uqe;
                        } else {
                            // Empty conjunction
                            return new Result(true, Collections.emptySet(), 0);
            }
                        }
                    Result result = handleConjunction(results, version);
        if (uqe != null) {
            result = result.unverify();
        }
        return result;
    }

    private static Result handleConjunction(List<Result> conjunctions, Version version) {
        if (conjunctions.isEmpty()) {
            throw new IllegalArgumentException("Must have at least on conjunction sub result");
        }
        for (Result subResult : conjunctions) {
            if (subResult.isMatchNoDocs()) {
                return subResult;
            }
        }
        int msm = 0;
        boolean verified = true;
        boolean matchAllDocs = true;
        boolean hasDuplicateTerms = false;
        Set<QueryExtraction> extractions = new HashSet<>();
        Set<String> seenRangeFields = new HashSet<>();
        for (Result result : conjunctions) {
            // In case that there are duplicate query extractions we need to be careful with
            // incrementing msm,
            // because that could lead to valid matches not becoming candidate matches:
            // query: (field:val1 AND field:val2) AND (field:val2 AND field:val3)
            // doc: field: val1 val2 val3
            // So lets be protective and decrease the msm:
            int resultMsm = result.minimumShouldMatch;
            for (QueryExtraction queryExtraction : result.extractions) {
                if (queryExtraction.range != null) {
                    // In case of range queries each extraction does not simply increment the
                    // minimum_should_match
                    // for that percolator query like for a term based extraction, so that can lead
                    // to more false
                    // positives for percolator queries with range queries than term based queries.
                    // The is because the way number fields are extracted from the document to be
                    // percolated.
                    // Per field a single range is extracted and if a percolator query has two or
                    // more range queries
                    // on the same field, then the minimum should match can be higher than clauses
                    // in the CoveringQuery.
                    // Therefore right now the minimum should match is incremented once per number
                    // field when processing
                    // the percolator query at index time.
                    if (seenRangeFields.add(queryExtraction.range.fieldName)) {
                        resultMsm = 1;
                    } else {
                        resultMsm = 0;
                    }
                }

                if (extractions.contains(queryExtraction)) {

                    resultMsm = 0;
                    verified = false;
                    break;
                }
            }
            msm += resultMsm;

            if (result.verified == false
                // If some inner extractions are optional, the result can't be verified
                || result.minimumShouldMatch < result.extractions.size()) {
                verified = false;
            }
            matchAllDocs &= result.matchAllDocs;
            extractions.addAll(result.extractions);
        }
        if (matchAllDocs) {
            return new Result(matchAllDocs, verified);
        } else {
            return new Result(verified, extractions, hasDuplicateTerms ? 1 : msm);
        }
    }

    private static Result handleDisjunctionQuery(List<Query> disjunctions, int requiredShouldClauses, Version version) {
        List<Result> subResults = new ArrayList<>();
        for (Query query : disjunctions) {
            // if either query fails extraction, we need to propagate as we could miss hits otherwise
            Result subResult = analyze(query, version);
            subResults.add(subResult);
        }
        return handleDisjunction(subResults, requiredShouldClauses, version);
    }

    private static Result handleDisjunction(List<Result> disjunctions, int requiredShouldClauses, Version version) {
        // Keep track of the msm for each clause:
        List<Integer> clauses = new ArrayList<>(disjunctions.size());
        boolean verified = true;
        int numMatchAllClauses = 0;
        boolean hasRangeExtractions = false;

        // In case that there are duplicate extracted terms / ranges then the msm should always be equal to the clause
        // with lowest msm, because the at percolate time there is no way to know the number of repetitions per
        // extracted term and field value from a percolator document may have more 'weight' than others.
        // Example percolator query: value1 OR value2 OR value2 OR value3 OR value3 OR value3 OR value4 OR value5 (msm set to 3)
        // In the above example query the extracted msm would be 3
        // Example document1: value1 value2 value3
        // With the msm and extracted terms this would match and is expected behaviour
        // Example document2: value3
        // This document should match too (value3 appears in 3 clauses), but with msm set to 3 and the fact
        // that fact that only distinct values are indexed in extracted terms field this document would
        // never match.
        boolean hasDuplicateTerms = false;

        Set<QueryExtraction> terms = new HashSet<>();
        for (int i = 0; i < disjunctions.size(); i++) {
            Result subResult = disjunctions.get(i);
            if (subResult.verified == false
                    // one of the sub queries requires more than one term to match, we can't
                    // verify it with a single top-level min_should_match
                    || subResult.minimumShouldMatch > 1
                    // One of the inner clauses has multiple extractions, we won't be able to
                    // verify it with a single top-level min_should_match
                    || (subResult.extractions.size() > 1 && requiredShouldClauses > 1)) {
                verified = false;
            }
            if (subResult.matchAllDocs) {
                numMatchAllClauses++;
            }
            int resultMsm = subResult.minimumShouldMatch;
            for (QueryExtraction extraction : subResult.extractions) {
                if (terms.add(extraction) == false) {
                    verified = false;
                    hasDuplicateTerms = true;
                }
            }
            if (hasRangeExtractions == false) {
                hasRangeExtractions = subResult.extractions.stream().anyMatch(qe -> qe.range != null);
            }
            clauses.add(resultMsm);
        }
        boolean matchAllDocs = numMatchAllClauses > 0 && numMatchAllClauses >= requiredShouldClauses;

        int msm = 0;
        // Having ranges would mean we need to juggle with the msm and that complicates this logic a lot,
        // so for now lets not do it.
        if (hasRangeExtractions == false) {
            // Figure out what the combined msm is for this disjunction:
            // (sum the lowest required clauses, otherwise we're too strict and queries may not match)
            clauses = clauses.stream()
                .filter(val -> val > 0)
                .sorted()
                .collect(Collectors.toList());

            // When there are duplicated query extractions, percolator can no longer reliably determine msm across this disjunction
            if (hasDuplicateTerms) {
                // pick lowest msm:
                msm = clauses.get(0);
            } else {
                int limit = Math.min(clauses.size(), Math.max(1, requiredShouldClauses));
                for (int i = 0; i < limit; i++) {
                    msm += clauses.get(i);
                }
            }
        } else {
            msm = 1;
        }
        if (matchAllDocs) {
            return new Result(matchAllDocs, verified);
        } else {
            return new Result(verified, terms, msm);
        }
    }

    /**
     * Query extraction result. A result is a candidate for a given document either if:
     *  - `matchAllDocs` is true
     *  - `extractions` and the document have `minimumShouldMatch` terms in common
     *  Further more, the match doesn't need to be verified if `verified` is true, checking
     *  `matchAllDocs` and `extractions` is enough.
     */
    static class Result {

        final Set<QueryExtraction> extractions;
        final boolean verified;
        final int minimumShouldMatch;
        final boolean matchAllDocs;

        private Result(boolean matchAllDocs, boolean verified, Set<QueryExtraction> extractions, int minimumShouldMatch) {
            if (minimumShouldMatch > extractions.size()) {
                throw new IllegalArgumentException("minimumShouldMatch can't be greater than the number of extractions: "
                        + minimumShouldMatch + " > " + extractions.size());
            }
            this.matchAllDocs = matchAllDocs;
            this.extractions = extractions;
            this.verified = verified;
            this.minimumShouldMatch = minimumShouldMatch;
        }

        Result(boolean verified, Set<QueryExtraction> extractions, int minimumShouldMatch) {
            this(false, verified, extractions, minimumShouldMatch);
        }

        Result(boolean matchAllDocs, boolean verified) {
            this(matchAllDocs, verified, Collections.emptySet(), 0);
        }

        Result unverify() {
            if (verified) {
                return new Result(matchAllDocs, false, extractions, minimumShouldMatch);
            } else {
                return this;
            }
        }

        boolean isMatchNoDocs() {
            return matchAllDocs == false && extractions.isEmpty();
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
