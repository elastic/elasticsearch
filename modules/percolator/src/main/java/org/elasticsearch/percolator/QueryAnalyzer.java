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
import org.apache.lucene.index.Term;
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
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

final class QueryAnalyzer {

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
        ResultBuilder builder = new ResultBuilder(false);
        query.visit(builder);
        return builder.getResult();
    }

    private static final Set<Class<?>> verifiedQueries = Set.of(
        TermQuery.class, TermInSetQuery.class, SynonymQuery.class, SpanTermQuery.class, SpanOrQuery.class,
        BooleanQuery.class, DisjunctionMaxQuery.class, ConstantScoreQuery.class, BoostQuery.class
    );

    private static boolean isVerified(Query query) {
        if (query instanceof FunctionScoreQuery) {
            return ((FunctionScoreQuery)query).getMinScore() == null;
        }
        return verifiedQueries.contains(query.getClass());
    }

    private static class ResultBuilder extends QueryVisitor {

        final boolean conjointTerms;
        List<ResultBuilder> conjunctions = new ArrayList<>();
        List<ResultBuilder> disjunctions = new ArrayList<>();
        boolean verified = true;
        int minimumShouldMatch = 0;
        List<Result> results = new ArrayList<>();

        private ResultBuilder(boolean conjointTerms) {
            this.conjointTerms = conjointTerms;
        }

        public Result getResult() {
            List<Result> partialResults = new ArrayList<>();
            if (results.size() > 0) {
                partialResults.add(conjointTerms ? handleConjunction(results) : handleDisjunction(results, minimumShouldMatch));
            }
            if (disjunctions.isEmpty() == false && (minimumShouldMatch > 0 || conjunctions.isEmpty())) {
                List<Result> childResults = disjunctions.stream().map(c -> c.getResult()).collect(Collectors.toList());
                if (childResults.size() > 1) {
                    partialResults.add(handleDisjunction(childResults, minimumShouldMatch));
                } else {
                    partialResults.add(childResults.get(0));
                }
            }
            if (conjunctions.isEmpty() == false) {
                List<Result> childResults = conjunctions.stream().map(c -> c.getResult()).collect(Collectors.toList());
                partialResults.addAll(childResults);
            }
            if (partialResults.isEmpty()) {
                return new Result(true, Collections.emptySet(), 0);
            }
            Result result;
            if (partialResults.size() == 1) {
                result = partialResults.get(0);
            } else {
                result = conjointTerms ? handleConjunction(partialResults) : handleDisjunction(partialResults, minimumShouldMatch);
            }
            if (verified == false) {
                result = result.unverify();
            }
            return result;
        }

        @Override
        public QueryVisitor getSubVisitor(Occur occur, Query parent) {
            System.out.println("getSubVisitor " + occur.name() + " " + parent);
            this.verified = isVerified(parent);
            if (occur == Occur.MUST || occur == Occur.FILTER) {
                ResultBuilder builder = new ResultBuilder(true);
                conjunctions.add(builder);
                return builder;
            }
            if (occur == Occur.MUST_NOT) {
                this.verified = false;
                return QueryVisitor.EMPTY_VISITOR;
            }
            ResultBuilder builder = new ResultBuilder(false);
            if (parent instanceof BooleanQuery) {
                builder.minimumShouldMatch = ((BooleanQuery)parent).getMinimumNumberShouldMatch();
            }
            disjunctions.add(builder);
            return builder;
        }

        @Override
        public void visitLeaf(Query query) {
            if (query instanceof MatchAllDocsQuery) {
                results.add(new Result(true, true));
            }
            else if (query instanceof MatchNoDocsQuery) {
                results.add(new Result(true, Collections.emptySet(), 0));
            }
            else if (query instanceof PointRangeQuery) {
                results.add(pointRangeQuery((PointRangeQuery)query));
            }
            else {
                results.add(Result.UNKNOWN);
            }
        }

        @Override
        public void consumeTerms(Query query, Term... terms) {
            System.out.println("Terms " + Arrays.toString(terms));
            boolean verified = verifiedQueries.contains(query.getClass());
            Set<QueryExtraction> qe = Arrays.stream(terms).map(QueryExtraction::new).collect(Collectors.toUnmodifiableSet());
            if (qe.size() > 0) {
                results.add(new Result(verified, qe, conjointTerms ? qe.size() : 1));
            }
        }

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

    private static Result pointRangeQuery(PointRangeQuery query) {
        if (query.getNumDims() != 1) {
            return Result.UNKNOWN;
        }

        byte[] lowerPoint = query.getLowerPoint();
        byte[] upperPoint = query.getUpperPoint();

        // Need to check whether upper is not smaller than lower, otherwise NumericUtils.subtract(...) fails IAE
        // If upper is really smaller than lower then we deal with like MatchNoDocsQuery. (verified and no extractions)
        if (new BytesRef(lowerPoint).compareTo(new BytesRef(upperPoint)) > 0) {
            return new Result(true, Collections.emptySet(), 0);
        }

        byte[] interval = new byte[16];
        NumericUtils.subtract(16, 0, prepad(upperPoint), prepad(lowerPoint), interval);
        return new Result(false, Collections.singleton(new QueryExtraction(
            new Range(query.getField(), lowerPoint, upperPoint, interval))), 1);
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
        List<Result> results = new ArrayList<>(conjunctions.size());
        for (Query query : conjunctions) {
            Result subResult = analyze(query, version);
            if (subResult.isMatchNoDocs()) {
                return subResult;
            }
            results.add(subResult);
        }
        return handleConjunction(results);
    }

    private static Result handleConjunction(List<Result> conjunctionsWithUnknowns) {
        List<Result> conjunctions = conjunctionsWithUnknowns.stream().filter(r -> r.isUnknown() == false).collect(Collectors.toList());
        if (conjunctions.isEmpty()) {
            if (conjunctionsWithUnknowns.isEmpty()) {
                throw new IllegalArgumentException("Must have at least on conjunction sub result");
            }
            return conjunctionsWithUnknowns.get(0); // all conjunctions are unknown, so just return the first one
        }
        if (conjunctionsWithUnknowns.size() == 1) {
            return conjunctionsWithUnknowns.get(0);
        }
        for (Result subResult : conjunctions) {
            if (subResult.isMatchNoDocs()) {
                return subResult;
            }
        }
        int msm = 0;
        boolean verified = conjunctionsWithUnknowns.size() == conjunctions.size();
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
        return handleDisjunction(subResults, requiredShouldClauses);
    }

    private static Result handleDisjunction(List<Result> disjunctions, int requiredShouldClauses) {
        if (disjunctions.stream().anyMatch(Result::isUnknown)) {
            return Result.UNKNOWN;
        }
        if (disjunctions.size() == 1) {
            return disjunctions.get(0);
        }
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

        boolean isUnknown() {
            return false;
        }

        boolean isMatchNoDocs() {
            return matchAllDocs == false && extractions.isEmpty();
        }

        static final Result UNKNOWN = new Result(false, false, Collections.emptySet(), 0){
            @Override
            boolean isUnknown() {
                return true;
            }

            @Override
            boolean isMatchNoDocs() {
                return false;
            }

            @Override
            public String toString() {
                return "UNKNOWN";
            }
        };
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
