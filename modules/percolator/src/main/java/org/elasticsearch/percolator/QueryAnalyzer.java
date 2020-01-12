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
import org.apache.lucene.queries.BlendedTermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
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
        BooleanQuery.class, DisjunctionMaxQuery.class, ConstantScoreQuery.class, BoostQuery.class,
        BlendedTermQuery.class
    );

    private static boolean isVerified(Query query) {
        if (query instanceof FunctionScoreQuery) {
            return ((FunctionScoreQuery)query).getMinScore() == null;
        }
        for (Class<?> cls : verifiedQueries) {
            if (cls.isAssignableFrom(query.getClass())) {
                return true;
            }
        }
        return false;
    }

    private static class ResultBuilder extends QueryVisitor {

        final boolean conjunction;
        List<ResultBuilder> children = new ArrayList<>();
        boolean verified = true;
        int minimumShouldMatch = 0;
        List<Result> terms = new ArrayList<>();

        private ResultBuilder(boolean conjunction) {
            this.conjunction = conjunction;
        }

        @Override
        public String toString() {
            return (conjunction ? "CONJ" : "DISJ") + children + terms + "~" + minimumShouldMatch;
        }

        Result getResult() {
            List<Result> partialResults = new ArrayList<>();
            if (terms.size() > 0) {
                partialResults.addAll(terms);
            }
            if (children.isEmpty() == false) {
                List<Result> childResults = children.stream().map(ResultBuilder::getResult).collect(Collectors.toList());
                partialResults.addAll(childResults);
            }
            if (partialResults.isEmpty()) {
                return verified ? Result.MATCH_NONE : Result.UNKNOWN;
            }
            Result result;
            if (partialResults.size() == 1) {
                result = partialResults.get(0);
            } else {
                result = conjunction ? handleConjunction(partialResults) : handleDisjunction(partialResults, minimumShouldMatch);
            }
            if (verified == false) {
                result = result.unverify();
            }
            return result;
        }

        @Override
        public QueryVisitor getSubVisitor(Occur occur, Query parent) {
            this.verified = isVerified(parent);
            if (occur == Occur.MUST || occur == Occur.FILTER) {
                ResultBuilder builder = new ResultBuilder(true);
                children.add(builder);
                return builder;
            }
            if (occur == Occur.MUST_NOT) {
                this.verified = false;
                return QueryVisitor.EMPTY_VISITOR;
            }
            int minimumShouldMatch = 0;
            if (parent instanceof BooleanQuery) {
                BooleanQuery bq = (BooleanQuery) parent;
                if (bq.getMinimumNumberShouldMatch() == 0
                    && bq.clauses().stream().anyMatch(c -> c.getOccur() == Occur.MUST || c.getOccur() == Occur.FILTER)) {
                    return QueryVisitor.EMPTY_VISITOR;
                }
                minimumShouldMatch = bq.getMinimumNumberShouldMatch();
            }
            ResultBuilder child = new ResultBuilder(false);
            child.minimumShouldMatch = minimumShouldMatch;
            children.add(child);
            return child;
        }

        @Override
        public void visitLeaf(Query query) {
            if (query instanceof MatchAllDocsQuery) {
                terms.add(new Result(true, true));
            }
            else if (query instanceof MatchNoDocsQuery) {
                terms.add(Result.MATCH_NONE);
            }
            else if (query instanceof PointRangeQuery) {
                terms.add(pointRangeQuery((PointRangeQuery)query));
            }
            else {
                terms.add(Result.UNKNOWN);
            }
        }

        @Override
        public void consumeTerms(Query query, Term... terms) {
            boolean verified = isVerified(query);
            Set<QueryExtraction> qe = Arrays.stream(terms).map(QueryExtraction::new).collect(Collectors.toUnmodifiableSet());
            if (qe.size() > 0) {
                this.terms.add(new Result(verified, qe, conjunction ? qe.size() : 1));
            }
        }

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

    private static Result handleConjunction(List<Result> conjunctionsWithUnknowns) {
        List<Result> conjunctions = conjunctionsWithUnknowns.stream().filter(r -> r.isUnknown() == false).collect(Collectors.toList());
        if (conjunctions.isEmpty()) {
            if (conjunctionsWithUnknowns.isEmpty()) {
                throw new IllegalArgumentException("Must have at least one conjunction sub result");
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
        Set<QueryExtraction> extractions = new HashSet<>();
        Set<String> seenRangeFields = new HashSet<>();
        for (Result result : conjunctions) {

            int resultMsm = result.minimumShouldMatch;
            for (QueryExtraction queryExtraction : result.extractions) {
                if (queryExtraction.range != null) {
                    // In case of range queries each extraction does not simply increment the
                    // minimum_should_match for that percolator query like for a term based extraction,
                    // so that can lead to more false positives for percolator queries with range queries
                    // than term based queries.
                    // This is because the way number fields are extracted from the document to be
                    // percolated.  Per field a single range is extracted and if a percolator query has two or
                    // more range queries on the same field, then the minimum should match can be higher than clauses
                    // in the CoveringQuery. Therefore right now the minimum should match is only incremented once per
                    // number field when processing the percolator query at index time.
                    // For multiple ranges within a single extraction (ie from an existing conjunction or disjunction)
                    // then this will already have been taken care of, so we only check against fieldnames from
                    // previously processed extractions, and don't add to the seenRangeFields list until all
                    // extractions from this result are processed
                    if (seenRangeFields.contains(queryExtraction.range.fieldName)) {
                        resultMsm = Math.max(0, resultMsm - 1);
                        verified = false;
                    }
                }
                else {
                    // In case that there are duplicate term query extractions we need to be careful with
                    // incrementing msm, because that could lead to valid matches not becoming candidate matches:
                    // query: (field:val1 AND field:val2) AND (field:val2 AND field:val3)
                    // doc: field: val1 val2 val3
                    // So lets be protective and decrease the msm:
                    if (extractions.contains(queryExtraction)) {
                        resultMsm = Math.max(0, resultMsm - 1);
                        verified = false;
                    }
                }
            }
            msm += resultMsm;

            // add range fields from this Result to the seenRangeFields set so that minimumShouldMatch is correctly
            // calculated for subsequent Results
            result.extractions.stream()
                .map(e -> e.range)
                .filter(Objects::nonNull)
                .map(e -> e.fieldName)
                .forEach(seenRangeFields::add);

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
            return new Result(verified, extractions, msm);
        }
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

        @Override
        public String toString() {
            return extractions.toString();
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

        static final Result MATCH_NONE = new Result(false, true, Collections.emptySet(), 0) {
            @Override
            boolean isMatchNoDocs() {
                return true;
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
