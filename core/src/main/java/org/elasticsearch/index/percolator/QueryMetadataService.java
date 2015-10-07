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
import org.elasticsearch.index.mapper.ParseContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility to extract query terms from queries and create queries from documents.
 */
public class QueryMetadataService {

    public static String QUERY_METADATA = "_query_metadata_";
    public static String QUERY_METADATA_FIELD_PREFIX = QUERY_METADATA + "field_";
    public static String QUERY_METADATA_FIELD_UNKNOWN = QUERY_METADATA + "unknown";
    public static FieldType QUERY_METADATA_FIELD_TYPE = new FieldType();

    static {
        QUERY_METADATA_FIELD_TYPE.setTokenized(false);
        QUERY_METADATA_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        QUERY_METADATA_FIELD_TYPE.freeze();
    }

    /**
     * Extracts all terms from the specified query and adds it to the specified document.
     */
    public void extractQueryMetadata(Query query, ParseContext.Document document) {
        List<Term> queryTerms;
        try {
            queryTerms = extractQueryMetadata(query);
        } catch (IllegalArgumentException e) {
            document.add(new Field(QUERY_METADATA_FIELD_UNKNOWN, new BytesRef(), QUERY_METADATA_FIELD_TYPE));
            return;
        }
        for (Term term : queryTerms) {
            document.add(new Field(QUERY_METADATA_FIELD_PREFIX + term.field(), term.bytes(), QUERY_METADATA_FIELD_TYPE));
        }
    }

    /**
     * Extracts all query terms from the provided query and adds it to specified list.
     *
     * From boolean query with no should clauses or phrase queries only the the longest term are selected,
     * since that those terms are likely to be the rarest. Boolean query's must_not clauses are always ignored ignored.
     *
     * If from part of the query, no query terms can be extracted then term extraction is stopped and
     * an IllegalArgumentException is thrown.
     */
    public List<Term> extractQueryMetadata(Query query) {
        if (query instanceof TermQuery) {
            return Collections.singletonList(((TermQuery) query).getTerm());
        } else if (query instanceof PhraseQuery) {
            Term[] terms = ((PhraseQuery) query).getTerms();
            if (terms.length == 0) {
                return Collections.emptyList();
            }

            // the longest term is likely to be the rarest,
            // so from a performance perspective it makes sense to extract that
            Term longestTerm = terms[0];
            for (Term term : terms) {
                if (longestTerm.bytes().length < term.bytes().length) {
                    longestTerm = term;
                }
            }
            return Collections.singletonList(longestTerm);
        } else if (query instanceof BooleanQuery) {
            List<BooleanClause> clauses = ((BooleanQuery) query).clauses();
            boolean hasShouldClauses = false;
            for (BooleanClause clause : clauses) {
                if (clause.getOccur() == BooleanClause.Occur.SHOULD) {
                    hasShouldClauses = true;
                    break;
                }
            }
            if (!hasShouldClauses) {
                List<Term> bestClause = null;
                for (BooleanClause clause : clauses) {
                    if (clause.isProhibited()) {
                        // we don't need to remember the things that do *not* match...
                        continue;
                    }

                    List<Term> temp = extractQueryMetadata(clause.getQuery());
                    bestClause = selectTermsListWithHighestSumOfTermLength(temp, bestClause);
                }
                if (bestClause != null) {
                    return bestClause;
                } else {
                    return Collections.emptyList();
                }
            } else {
                List<Term> terms = new ArrayList<>();
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
            throw new IllegalArgumentException("unsupported query");
        }
    }

    List<Term> selectTermsListWithHighestSumOfTermLength(List<Term> terms1, List<Term> terms2) {
        if (terms1 == null) {
            return terms2;
        } else if (terms2 == null) {
            return terms1;
        } else {
            int terms1SumTermLength = computeSumOfTermLength(terms1);
            int terms2SumTermLength = computeSumOfTermLength(terms2);
            // keep the clause with longest terms, this likely to be rarest.
            if (terms1SumTermLength > terms2SumTermLength) {
                return terms1;
            } else {
                return terms2;
            }
        }
    }

    private int computeSumOfTermLength(List<Term> terms) {
        int sum = 0;
        for (Term term : terms) {
            sum += term.bytes().length;
        }
        return sum;
    }

    /**
     * Creates a boolean query with a should clause for each term on all fields of the specified index reader.
     */
    public Query createQueryMetadataQuery(IndexReader indexReader) throws IOException {
        List<Term> extractedTerms = new ArrayList<>();
        extractedTerms.add(new Term(QUERY_METADATA_FIELD_UNKNOWN));
        Fields fields = MultiFields.getFields(indexReader);
        for (String field : fields) {
            Terms terms = fields.terms(field);
            if (terms == null) {
                continue;
            }

            TermsEnum tenum = terms.iterator();
            for (BytesRef term = tenum.next(); term != null ; term = tenum.next()) {
                extractedTerms.add(new Term(QUERY_METADATA_FIELD_PREFIX + field, BytesRef.deepCopyOf(term)));
            }
        }
        return new TermsQuery(extractedTerms);
    }

}
