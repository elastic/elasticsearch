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
package org.elasticsearch.index.percolator;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.logging.support.LoggerMessageFormat;
import org.elasticsearch.index.mapper.ParseContext;

import java.io.IOException;
import java.util.*;

/**
 * Utility to extract query terms from queries and create queries from documents.
 */
public final class QueryMetadataService {

    private static final byte FIELD_VALUE_SEPARATOR = 0;  // nul code point

    public static String QUERY_METADATA_FIELD = "query_metadata_field";
    public static String QUERY_METADATA_FIELD_UNKNOWN = "query_metadata_field_unknown";
    public static FieldType QUERY_METADATA_FIELD_TYPE = new FieldType();

    static {
        QUERY_METADATA_FIELD_TYPE.setTokenized(false);
        QUERY_METADATA_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        QUERY_METADATA_FIELD_TYPE.freeze();
    }

    private QueryMetadataService() {
    }

    /**
     * Extracts all terms from the specified query and adds it to the specified document.
     */
    public static void extractQueryMetadata(Query query, ParseContext.Document document) {
        Set<Term> queryTerms;
        try {
            queryTerms = extractQueryMetadata(query);
        } catch (UnsupportedQueryException e) {
            document.add(new Field(QUERY_METADATA_FIELD_UNKNOWN, new BytesRef(), QUERY_METADATA_FIELD_TYPE));
            return;
        }
        for (Term term : queryTerms) {
            BytesRefBuilder builder = new BytesRefBuilder();
            builder.append(new BytesRef(term.field()));
            builder.append(FIELD_VALUE_SEPARATOR);
            builder.append(term.bytes());
            document.add(new Field(QUERY_METADATA_FIELD, builder.toBytesRef(), QUERY_METADATA_FIELD_TYPE));
        }
    }

    /**
     * Extracts all query terms from the provided query and adds it to specified list.
     *
     * From boolean query with no should clauses or phrase queries only the the longest term are selected,
     * since that those terms are likely to be the rarest. Boolean query's must_not clauses are always ignored ignored.
     *
     * If from part of the query, no query terms can be extracted then term extraction is stopped and
     * an UnsupportedQueryException is thrown.
     */
    public static Set<Term> extractQueryMetadata(Query query) {
        // TODO: add support for the TermsQuery when it has methods to access the actual terms it encapsulates
        // TODO: add support for span queries
        if (query instanceof TermQuery) {
            return Collections.singleton(((TermQuery) query).getTerm());
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
                Set<Term> bestClause = null;
                for (BooleanClause clause : clauses) {
                    if (clause.isRequired() == false) {
                        // skip must_not clauses, we don't need to remember the things that do *not* match...
                        // skip should clauses, this bq has must clauses, so we don't need to remember should clauses, since they are completely optional.
                        continue;
                    }

                    Set<Term> temp = extractQueryMetadata(clause.getQuery());
                    bestClause = selectTermListWithTheLongestShortestTerm(temp, bestClause);
                }
                if (bestClause != null) {
                    return bestClause;
                } else {
                    return Collections.emptySet();
                }
            } else {
                Set<Term> terms = new HashSet<>();
                for (BooleanClause clause : clauses) {
                    if (clause.isProhibited()) {
                        // we don't need to remember the things that do *not* match...
                        continue;
                    }
                    terms.addAll(extractQueryMetadata(clause.getQuery()));
                }
                return terms;
            }
        } else if (query instanceof ConstantScoreQuery) {
            Query wrappedQuery = ((ConstantScoreQuery) query).getQuery();
            return extractQueryMetadata(wrappedQuery);
        } else if (query instanceof BoostQuery) {
            Query wrappedQuery = ((BoostQuery) query).getQuery();
            return extractQueryMetadata(wrappedQuery);
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
            if (terms1ShortestTerm > terms2ShortestTerm) {
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
    public static Query createQueryMetadataQuery(IndexReader indexReader) throws IOException {
        List<Term> extractedTerms = new ArrayList<>();
        extractedTerms.add(new Term(QUERY_METADATA_FIELD_UNKNOWN));
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
                extractedTerms.add(new Term(QUERY_METADATA_FIELD, builder.toBytesRef()));
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
