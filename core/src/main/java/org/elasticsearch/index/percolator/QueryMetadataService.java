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
import java.util.List;

/**
 *
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

    public void extractQueryMetadata(Query query, ParseContext.Document document) {
        List<Term> queryTerms = new ArrayList<>();
        extractQueryMetadata(query, queryTerms);
        for (Term term : queryTerms) {
            document.add(new Field(term.field(), term.bytes(), QUERY_METADATA_FIELD_TYPE));
        }
    }

    public void extractQueryMetadata(Query query, List<Term> queryTerms) {
        if (query instanceof TermQuery) {
            Term term = ((TermQuery) query).getTerm();
            queryTerms.add(new Term(QUERY_METADATA_FIELD_PREFIX + term.field(), term.bytes()));
        } else if (query instanceof PhraseQuery) {
            Term[] terms = ((PhraseQuery) query).getTerms();
            if (terms.length == 0) {
                return;
            }

            Term longestTerm = terms[0];
            for (Term term : terms) {
                if (longestTerm.bytes().length < term.bytes().length) {
                    longestTerm = term;
                }
            }
            queryTerms.add(new Term(QUERY_METADATA_FIELD_PREFIX + longestTerm.field(), longestTerm.bytes()));
        } else if (query instanceof BooleanQuery) {
            List<BooleanClause> clauses = ((BooleanQuery) query).clauses();
            boolean noShouldClauses = true;
            for (BooleanClause clause : clauses) {
                if (clause.getOccur() == BooleanClause.Occur.SHOULD) {
                    noShouldClauses = false;
                    break;
                }
            }
            if (noShouldClauses) {
                List<Term> bestClause = null;
                for (BooleanClause clause : clauses) {
                    if (clause.isProhibited()) {
                        // we don't need to remember the things that do *not* match...
                        continue;
                    }

                    List<Term> temp = new ArrayList<>();
                    extractQueryMetadata(clause.getQuery(), temp);
                    if (bestClause == null) {
                        bestClause = temp;
                    } else {
                        int currentSize = 0;
                        for (Term term : bestClause) {
                            currentSize += term.bytes().length;
                        }
                        int otherSize = 0;
                        for (Term term : temp) {
                            otherSize += term.bytes().length;
                        }
                        // keep the clause with longest terms, this likely to be rarest.
                        if (otherSize > currentSize) {
                            bestClause = temp;
                        }
                    }
                }
                if (bestClause != null) {
                    queryTerms.addAll(bestClause);
                }
            } else {
                for (BooleanClause clause : clauses) {
                    if (clause.isProhibited()) {
                        // we don't need to remember the things that do *not* match...
                        continue;
                    }
                    extractQueryMetadata(clause.getQuery(), queryTerms);
                }
            }
        } else if (query instanceof ConstantScoreQuery) {
            Query wrappedQuery = ((ConstantScoreQuery) query).getQuery();
            extractQueryMetadata(wrappedQuery, queryTerms);
        } else if (query instanceof BoostQuery) {
            Query wrappedQuery = ((BoostQuery) query).getQuery();
            extractQueryMetadata(wrappedQuery, queryTerms);
        } else {
            queryTerms.clear();
            queryTerms.add(new Term(QUERY_METADATA_FIELD_UNKNOWN));
        }
    }

    public Query createQueryMetadataQuery(IndexReader indexReader) throws IOException {
        List<Term> extractedTerms = new ArrayList<>();
        extractedTerms.add(new Term(QUERY_METADATA_FIELD_UNKNOWN));
        Fields fields = MultiFields.getFields(indexReader);
        for (String field : fields) {
            // Ignore meta fields
            if (field.startsWith("_")) {
                continue;
            }

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
