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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.BlendedTermQuery;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.index.mapper.ParseContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Utility to extract query terms from queries and create queries from documents.
 */
public final class ExtractQueryTermsService {

    private static final byte FIELD_VALUE_SEPARATOR = 0;  // nul code point

    private ExtractQueryTermsService() {
    }

    /**
     * Extracts all terms from the specified query and adds it to the specified document.
     * @param query                 The query to extract terms from
     * @param document              The document to add the extracted terms to
     * @param queryTermsFieldField  The field in the document holding the extracted terms
     * @param unknownQueryField     The field used to mark a document that not all query terms could be extracted.
     *                              For example the query contained an unsupported query (e.g. WildcardQuery).
     * @param fieldType The field type for the query metadata field
     */
    public static void extractQueryTerms(Query query, ParseContext.Document document, String queryTermsFieldField,
                                         String unknownQueryField, FieldType fieldType) {
        Set<Term> queryTerms;
        try {
            queryTerms = extractQueryTerms(query);
        } catch (UnsupportedQueryException e) {
            document.add(new Field(unknownQueryField, new BytesRef(), fieldType));
            return;
        }
        for (Term term : queryTerms) {
            BytesRefBuilder builder = new BytesRefBuilder();
            builder.append(new BytesRef(term.field()));
            builder.append(FIELD_VALUE_SEPARATOR);
            builder.append(term.bytes());
            document.add(new Field(queryTermsFieldField, builder.toBytesRef(), fieldType));
        }
    }

    /**
     * Extracts all query terms from the provided query and adds it to specified list.
     *
     * From boolean query with no should clauses or phrase queries only the longest term are selected,
     * since that those terms are likely to be the rarest. Boolean query's must_not clauses are always ignored.
     *
     * If from part of the query, no query terms can be extracted then term extraction is stopped and
     * an UnsupportedQueryException is thrown.
     */
    static Set<Term> extractQueryTerms(Query query) {
        if (query instanceof MatchNoDocsQuery) {
            // no terms to extract as this query matches no docs
            return Collections.emptySet();
        } else if (query instanceof TermQuery) {
            return Collections.singleton(((TermQuery) query).getTerm());
        } else if (query instanceof TermsQuery) {
            Set<Term> terms = new HashSet<>();
            TermsQuery termsQuery = (TermsQuery) query;
            PrefixCodedTerms.TermIterator iterator = termsQuery.getTermData().iterator();
            for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
                terms.add(new Term(iterator.field(), term));
            }
            return terms;
        } else if (query instanceof PhraseQuery) {
            Term[] terms = ((PhraseQuery) query).getTerms();
            if (terms.length == 0) {
                return Collections.emptySet();
            }

            // the longest term is likely to be the rarest,
            // so from a performance perspective it makes sense to extract that
            Term longestTerm = terms[0];
            for (Term term : terms) {
                if (longestTerm.bytes().length < term.bytes().length) {
                    longestTerm = term;
                }
            }
            return Collections.singleton(longestTerm);
        } else if (query instanceof BooleanQuery) {
            List<BooleanClause> clauses = ((BooleanQuery) query).clauses();
            boolean hasRequiredClauses = false;
            for (BooleanClause clause : clauses) {
                if (clause.isRequired()) {
                    hasRequiredClauses = true;
                    break;
                }
            }
            if (hasRequiredClauses) {
                UnsupportedQueryException uqe = null;
                Set<Term> bestClause = null;
                for (BooleanClause clause : clauses) {
                    if (clause.isRequired() == false) {
                        // skip must_not clauses, we don't need to remember the things that do *not* match...
                        // skip should clauses, this bq has must clauses, so we don't need to remember should clauses,
                        // since they are completely optional.
                        continue;
                    }

                    Set<Term> temp;
                    try {
                        temp = extractQueryTerms(clause.getQuery());
                    } catch (UnsupportedQueryException e) {
                        uqe = e;
                        continue;
                    }
                    bestClause = selectTermListWithTheLongestShortestTerm(temp, bestClause);
                }
                if (bestClause != null) {
                    return bestClause;
                } else {
                    if (uqe != null) {
                        throw uqe;
                    }
                    return Collections.emptySet();
                }
            } else {
                Set<Term> terms = new HashSet<>();
                for (BooleanClause clause : clauses) {
                    if (clause.isProhibited()) {
                        // we don't need to remember the things that do *not* match...
                        continue;
                    }
                    terms.addAll(extractQueryTerms(clause.getQuery()));
                }
                return terms;
            }
        } else if (query instanceof ConstantScoreQuery) {
            Query wrappedQuery = ((ConstantScoreQuery) query).getQuery();
            return extractQueryTerms(wrappedQuery);
        } else if (query instanceof BoostQuery) {
            Query wrappedQuery = ((BoostQuery) query).getQuery();
            return extractQueryTerms(wrappedQuery);
        } else if (query instanceof CommonTermsQuery) {
            List<Term> terms = ((CommonTermsQuery) query).getTerms();
            return new HashSet<>(terms);
        } else if (query instanceof BlendedTermQuery) {
            List<Term> terms = ((BlendedTermQuery) query).getTerms();
            return new HashSet<>(terms);
        } else if (query instanceof DisjunctionMaxQuery) {
            List<Query> disjuncts = ((DisjunctionMaxQuery) query).getDisjuncts();
            Set<Term> terms = new HashSet<>();
            for (Query disjunct : disjuncts) {
                terms.addAll(extractQueryTerms(disjunct));
            }
            return terms;
        } else if (query instanceof SpanTermQuery) {
            return Collections.singleton(((SpanTermQuery) query).getTerm());
        } else if (query instanceof SpanNearQuery) {
            Set<Term> bestClause = null;
            SpanNearQuery spanNearQuery = (SpanNearQuery) query;
            for (SpanQuery clause : spanNearQuery.getClauses()) {
                Set<Term> temp = extractQueryTerms(clause);
                bestClause = selectTermListWithTheLongestShortestTerm(temp, bestClause);
            }
            return bestClause;
        } else if (query instanceof SpanOrQuery) {
            Set<Term> terms = new HashSet<>();
            SpanOrQuery spanOrQuery = (SpanOrQuery) query;
            for (SpanQuery clause : spanOrQuery.getClauses()) {
                terms.addAll(extractQueryTerms(clause));
            }
            return terms;
        } else if (query instanceof SpanFirstQuery) {
            return extractQueryTerms(((SpanFirstQuery)query).getMatch());
        } else if (query instanceof SpanNotQuery) {
            return extractQueryTerms(((SpanNotQuery) query).getInclude());
        } else {
            throw new UnsupportedQueryException(query);
        }
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

    private static int minTermLength(Set<Term> terms) {
        int min = Integer.MAX_VALUE;
        for (Term term : terms) {
            min = Math.min(min, term.bytes().length);
        }
        return min;
    }

    /**
     * Creates a boolean query with a should clause for each term on all fields of the specified index reader.
     */
    public static Query createQueryTermsQuery(IndexReader indexReader, String queryMetadataField,
                                              String unknownQueryField) throws IOException {
        Objects.requireNonNull(queryMetadataField);
        Objects.requireNonNull(unknownQueryField);

        List<Term> extractedTerms = new ArrayList<>();
        extractedTerms.add(new Term(unknownQueryField));
        Fields fields = MultiFields.getFields(indexReader);
        for (String field : fields) {
            Terms terms = fields.terms(field);
            if (terms == null) {
                continue;
            }

            BytesRef fieldBr = new BytesRef(field);
            TermsEnum tenum = terms.iterator();
            for (BytesRef term = tenum.next(); term != null ; term = tenum.next()) {
                BytesRefBuilder builder = new BytesRefBuilder();
                builder.append(fieldBr);
                builder.append(FIELD_VALUE_SEPARATOR);
                builder.append(term);
                extractedTerms.add(new Term(queryMetadataField, builder.toBytesRef()));
            }
        }
        return new TermsQuery(extractedTerms);
    }

    /**
     * Exception indicating that none or some query terms couldn't extracted from a percolator query.
     */
    public static class UnsupportedQueryException extends RuntimeException {

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
