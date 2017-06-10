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

import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.BlendedTermQuery;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
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
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public final class QueryAnalyzer {

    private static final Map<Class<? extends Query>, Function<Query, Result>> queryProcessors;

    static {
        Map<Class<? extends Query>, Function<Query, Result>> map = new HashMap<>();
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
        queryProcessors = Collections.unmodifiableMap(map);
    }

    private QueryAnalyzer() {
    }

    /**
     * Extracts terms from the provided query. These terms are stored with the percolator query and
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
     * Sometimes the query analyzer can't always extract terms from a sub query, if that happens then
     * query analysis is stopped and an UnsupportedQueryException is thrown. So that the caller can mark
     * this query in such a way that the PercolatorQuery always verifies if this query with the MemoryIndex.
     */
    public static Result analyze(Query query) {
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
        return (query -> new Result(true, Collections.emptySet()));
    }

    static Function<Query, Result> constantScoreQuery() {
        return query -> {
            Query wrappedQuery = ((ConstantScoreQuery) query).getQuery();
            return analyze(wrappedQuery);
        };
    }

    static Function<Query, Result> boostQuery() {
        return query -> {
            Query wrappedQuery = ((BoostQuery) query).getQuery();
            return analyze(wrappedQuery);
        };
    }

    static Function<Query, Result> termQuery() {
        return (query -> {
            TermQuery termQuery = (TermQuery) query;
            return new Result(true, Collections.singleton(termQuery.getTerm()));
        });
    }

    static Function<Query, Result> termInSetQuery() {
        return query -> {
            TermInSetQuery termInSetQuery = (TermInSetQuery) query;
            Set<Term> terms = new HashSet<>();
            PrefixCodedTerms.TermIterator iterator = termInSetQuery.getTermData().iterator();
            for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
                terms.add(new Term(iterator.field(), term));
            }
            return new Result(true, terms);
        };
    }

    static Function<Query, Result> synonymQuery() {
        return query -> {
            Set<Term> terms = new HashSet<>(((SynonymQuery) query).getTerms());
            return new Result(true, terms);
        };
    }

    static Function<Query, Result> commonTermsQuery() {
        return query -> {
            List<Term> terms = ((CommonTermsQuery) query).getTerms();
            return new Result(false, new HashSet<>(terms));
        };
    }

    static Function<Query, Result> blendedTermQuery() {
        return query -> {
            List<Term> terms = ((BlendedTermQuery) query).getTerms();
            return new Result(true, new HashSet<>(terms));
        };
    }

    static Function<Query, Result> phraseQuery() {
        return query -> {
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
            return new Result(false, Collections.singleton(longestTerm));
        };
    }

    static Function<Query, Result> multiPhraseQuery() {
        return query -> {
            Term[][] terms = ((MultiPhraseQuery) query).getTermArrays();
            if (terms.length == 0) {
                return new Result(true, Collections.emptySet());
            }

            Set<Term> bestTermArr = null;
            for (Term[] termArr : terms) {
                bestTermArr = selectTermListWithTheLongestShortestTerm(bestTermArr, new HashSet<>(Arrays.asList(termArr)));
            }
            return new Result(false, bestTermArr);
        };
    }

    static Function<Query, Result> spanTermQuery() {
        return query -> {
            Term term = ((SpanTermQuery) query).getTerm();
            return new Result(true, Collections.singleton(term));
        };
    }

    static Function<Query, Result> spanNearQuery() {
        return query -> {
            Set<Term> bestClauses = null;
            SpanNearQuery spanNearQuery = (SpanNearQuery) query;
            for (SpanQuery clause : spanNearQuery.getClauses()) {
                Result temp = analyze(clause);
                bestClauses = selectTermListWithTheLongestShortestTerm(temp.terms, bestClauses);
            }
            return new Result(false, bestClauses);
        };
    }

    static Function<Query, Result> spanOrQuery() {
        return query -> {
            Set<Term> terms = new HashSet<>();
            SpanOrQuery spanOrQuery = (SpanOrQuery) query;
            for (SpanQuery clause : spanOrQuery.getClauses()) {
                terms.addAll(analyze(clause).terms);
            }
            return new Result(false, terms);
        };
    }

    static Function<Query, Result> spanNotQuery() {
        return query -> {
            Result result = analyze(((SpanNotQuery) query).getInclude());
            return new Result(false, result.terms);
        };
    }

    static Function<Query, Result> spanFirstQuery() {
        return query -> {
            Result result = analyze(((SpanFirstQuery) query).getMatch());
            return new Result(false, result.terms);
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
                Set<Term> bestClause = null;
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
                        temp = analyze(clause.getQuery());
                    } catch (UnsupportedQueryException e) {
                        uqe = e;
                        continue;
                    }
                    bestClause = selectTermListWithTheLongestShortestTerm(temp.terms, bestClause);
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

    static Function<Query, Result> functionScoreQuery() {
        return query -> {
            FunctionScoreQuery functionScoreQuery = (FunctionScoreQuery) query;
            Result result = analyze(functionScoreQuery.getSubQuery());
            // If min_score is specified we can't guarantee upfront that this percolator query matches,
            // so in that case we set verified to false.
            // (if it matches with the percolator document matches with the extracted terms.
            // Min score filters out docs, which is different than the functions, which just influences the score.)
            boolean verified = functionScoreQuery.getMinScore() == null;
            return new Result(verified, result.terms);
        };
    }

    static Result handleDisjunction(List<Query> disjunctions, int minimumShouldMatch, boolean otherClauses) {
        boolean verified = minimumShouldMatch <= 1 && otherClauses == false;
        Set<Term> terms = new HashSet<>();
        for (Query disjunct : disjunctions) {
            Result subResult = analyze(disjunct);
            if (subResult.verified == false) {
                verified = false;
            }
            terms.addAll(subResult.terms);
        }
        return new Result(verified, terms);
    }

    static Set<Term> selectTermListWithTheLongestShortestTerm(Set<Term> terms1, Set<Term> terms2) {
        if (terms1 == null) {
            return terms2;
        } else if (terms2 == null) {
            return terms1;
        } else {
            int terms1ShortestTerm = minTermLength(terms1);
            int terms2ShortestTerm = minTermLength(terms2);
            // keep the clause with longest terms, this likely to be rarest.
            if (terms1ShortestTerm >= terms2ShortestTerm) {
                return terms1;
            } else {
                return terms2;
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

        Result(boolean verified, Set<Term> terms) {
            this.terms = terms;
            this.verified = verified;
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
        public Query getUnsupportedQuery() {
            return unsupportedQuery;
        }
    }

}
