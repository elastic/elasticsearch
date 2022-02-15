/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.core;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TermVectorsResponse {
    private final String index;
    private final String id;
    private final long docVersion;
    private final boolean found;
    private final long tookInMillis;
    private final List<TermVector> termVectorList;

    public TermVectorsResponse(String index, String id, long version, boolean found, long tookInMillis, List<TermVector> termVectorList) {
        this.index = index;
        this.id = id;
        this.docVersion = version;
        this.found = found;
        this.tookInMillis = tookInMillis;
        this.termVectorList = termVectorList;
    }

    private static final ConstructingObjectParser<TermVectorsResponse, Void> PARSER = new ConstructingObjectParser<>(
        "term_vectors",
        true,
        args -> {
            // as the response comes from server, we are sure that args[5] will be a list of TermVector
            @SuppressWarnings("unchecked")
            List<TermVector> termVectorList = (List<TermVector>) args[5];
            if (termVectorList != null) {
                Collections.sort(termVectorList, Comparator.comparing(TermVector::getFieldName));
            }
            return new TermVectorsResponse(
                (String) args[0],
                (String) args[1],
                (long) args[2],
                (boolean) args[3],
                (long) args[4],
                termVectorList
            );
        }
    );

    static {
        PARSER.declareString(constructorArg(), new ParseField("_index"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("_id"));
        PARSER.declareLong(constructorArg(), new ParseField("_version"));
        PARSER.declareBoolean(constructorArg(), new ParseField("found"));
        PARSER.declareLong(constructorArg(), new ParseField("took"));
        PARSER.declareNamedObjects(
            optionalConstructorArg(),
            (p, c, fieldName) -> TermVector.fromXContent(p, fieldName),
            new ParseField("term_vectors")
        );
    }

    public static TermVectorsResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    /**
     * Returns the index for the response
     */
    public String getIndex() {
        return index;
    }

    /**
     * Returns the id of the request
     * can be NULL if there is no document ID
     */
    public String getId() {
        return id;
    }

    /**
     * Returns if the document is found
     * always <code>true</code> for artificial documents
     */
    public boolean getFound() {
        return found;
    }

    /**
     * Returns the document version
     */
    public long getDocVersion() {
        return docVersion;
    }

    /**
     * Returns the time that a request took in milliseconds
     */
    public long getTookInMillis() {
        return tookInMillis;
    }

    /**
     * Returns the list of term vectors
     */
    public List<TermVector> getTermVectorsList() {
        return termVectorList;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if ((obj instanceof TermVectorsResponse) == false) return false;
        TermVectorsResponse other = (TermVectorsResponse) obj;
        return index.equals(other.index)
            && Objects.equals(id, other.id)
            && docVersion == other.docVersion
            && found == other.found
            && tookInMillis == other.tookInMillis
            && Objects.equals(termVectorList, other.termVectorList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, id, docVersion, found, tookInMillis, termVectorList);
    }

    public static final class TermVector {

        private static final ConstructingObjectParser<TermVector, String> PARSER = new ConstructingObjectParser<>(
            "term_vector",
            true,
            (args, ctxFieldName) -> {
                // as the response comes from server, we are sure that args[1] will be a list of Term
                @SuppressWarnings("unchecked")
                List<Term> terms = (List<Term>) args[1];
                if (terms != null) {
                    Collections.sort(terms, Comparator.comparing(Term::getTerm));
                }
                return new TermVector(ctxFieldName, (FieldStatistics) args[0], terms);
            }
        );

        static {
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> FieldStatistics.fromXContent(p), new ParseField("field_statistics"));
            PARSER.declareNamedObjects(optionalConstructorArg(), (p, c, term) -> Term.fromXContent(p, term), new ParseField("terms"));
        }

        private final String fieldName;
        @Nullable
        private final FieldStatistics fieldStatistics;
        @Nullable
        private final List<Term> terms;

        public TermVector(String fieldName, FieldStatistics fieldStatistics, List<Term> terms) {
            this.fieldName = fieldName;
            this.fieldStatistics = fieldStatistics;
            this.terms = terms;
        }

        public static TermVector fromXContent(XContentParser parser, String fieldName) {
            return PARSER.apply(parser, fieldName);
        }

        /**
         * Returns the field name of the current term vector
         */
        public String getFieldName() {
            return fieldName;
        }

        /**
         * Returns the list of terms for the current term vector
         */
        public List<Term> getTerms() {
            return terms;
        }

        /**
         * Returns the field statistics for the current field
         */
        public FieldStatistics getFieldStatistics() {
            return fieldStatistics;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if ((obj instanceof TermVector) == false) return false;
            TermVector other = (TermVector) obj;
            return fieldName.equals(other.fieldName)
                && Objects.equals(fieldStatistics, other.fieldStatistics)
                && Objects.equals(terms, other.terms);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, fieldStatistics, terms);
        }

        // Class containing a general field statistics for the field
        public static final class FieldStatistics {

            private static final ConstructingObjectParser<FieldStatistics, Void> PARSER = new ConstructingObjectParser<>(
                "field_statistics",
                true,
                args -> { return new FieldStatistics((long) args[0], (int) args[1], (long) args[2]); }
            );

            static {
                PARSER.declareLong(constructorArg(), new ParseField("sum_doc_freq"));
                PARSER.declareInt(constructorArg(), new ParseField("doc_count"));
                PARSER.declareLong(constructorArg(), new ParseField("sum_ttf"));
            }
            private final long sumDocFreq;
            private final int docCount;
            private final long sumTotalTermFreq;

            public FieldStatistics(long sumDocFreq, int docCount, long sumTotalTermFreq) {
                this.sumDocFreq = sumDocFreq;
                this.docCount = docCount;
                this.sumTotalTermFreq = sumTotalTermFreq;
            }

            public static FieldStatistics fromXContent(XContentParser parser) {
                return PARSER.apply(parser, null);
            }

            /*
             * Returns how many documents this field contains
             */
            public int getDocCount() {
                return docCount;
            }

            /**
             * Returns the sum of document frequencies for all terms in this field
             */
            public long getSumDocFreq() {
                return sumDocFreq;
            }

            /**
             * Returns the sum of total term frequencies of all terms in this field
             */
            public long getSumTotalTermFreq() {
                return sumTotalTermFreq;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) return true;
                if ((obj instanceof FieldStatistics) == false) return false;
                FieldStatistics other = (FieldStatistics) obj;
                return docCount == other.docCount && sumDocFreq == other.sumDocFreq && sumTotalTermFreq == other.sumTotalTermFreq;
            }

            @Override
            public int hashCode() {
                return Objects.hash(docCount, sumDocFreq, sumTotalTermFreq);
            }
        }

        public static final class Term {
            private static final ConstructingObjectParser<Term, String> PARSER = new ConstructingObjectParser<>(
                "token",
                true,
                (args, ctxTerm) -> {
                    // as the response comes from server, we are sure that args[4] will be a list of Token
                    @SuppressWarnings("unchecked")
                    List<Token> tokens = (List<Token>) args[4];
                    if (tokens != null) {
                        Collections.sort(
                            tokens,
                            Comparator.comparing(Token::getPosition, Comparator.nullsFirst(Integer::compareTo))
                                .thenComparing(Token::getStartOffset, Comparator.nullsFirst(Integer::compareTo))
                                .thenComparing(Token::getEndOffset, Comparator.nullsFirst(Integer::compareTo))
                        );
                    }
                    return new Term(ctxTerm, (int) args[0], (Integer) args[1], (Long) args[2], (Float) args[3], tokens);
                }
            );
            static {
                PARSER.declareInt(constructorArg(), new ParseField("term_freq"));
                PARSER.declareInt(optionalConstructorArg(), new ParseField("doc_freq"));
                PARSER.declareLong(optionalConstructorArg(), new ParseField("ttf"));
                PARSER.declareFloat(optionalConstructorArg(), new ParseField("score"));
                PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> Token.fromXContent(p), new ParseField("tokens"));
            }

            private final String term;
            private final int termFreq;
            @Nullable
            private final Integer docFreq;
            @Nullable
            private final Long totalTermFreq;
            @Nullable
            private final Float score;
            @Nullable
            private final List<Token> tokens;

            public Term(String term, int termFreq, Integer docFreq, Long totalTermFreq, Float score, List<Token> tokens) {
                this.term = term;
                this.termFreq = termFreq;
                this.docFreq = docFreq;
                this.totalTermFreq = totalTermFreq;
                this.score = score;
                this.tokens = tokens;
            }

            public static Term fromXContent(XContentParser parser, String term) {
                return PARSER.apply(parser, term);
            }

            /**
             * Returns the string representation of the term
             */
            public String getTerm() {
                return term;
            }

            /**
             * Returns term frequency - the number of times this term occurs in the current document
             */
            public int getTermFreq() {
                return termFreq;
            }

            /**
             * Returns document frequency - the number of documents in the index that contain this term
             */
            public Integer getDocFreq() {
                return docFreq;
            }

            /**
             * Returns total term frequency - the number of times this term occurs across all documents
             */
            public Long getTotalTermFreq() {
                return totalTermFreq;
            }

            /**
             * Returns tf-idf score, if the request used some form of terms filtering
             */
            public Float getScore() {
                return score;
            }

            /**
             * Returns a list of tokens for the term
             */
            public List<Token> getTokens() {
                return tokens;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) return true;
                if ((obj instanceof Term) == false) return false;
                Term other = (Term) obj;
                return term.equals(other.term)
                    && termFreq == other.termFreq
                    && Objects.equals(docFreq, other.docFreq)
                    && Objects.equals(totalTermFreq, other.totalTermFreq)
                    && Objects.equals(score, other.score)
                    && Objects.equals(tokens, other.tokens);
            }

            @Override
            public int hashCode() {
                return Objects.hash(term, termFreq, docFreq, totalTermFreq, score, tokens);
            }
        }

        public static final class Token {

            private static final ConstructingObjectParser<Token, Void> PARSER = new ConstructingObjectParser<>(
                "token",
                true,
                args -> { return new Token((Integer) args[0], (Integer) args[1], (Integer) args[2], (String) args[3]); }
            );
            static {
                PARSER.declareInt(optionalConstructorArg(), new ParseField("start_offset"));
                PARSER.declareInt(optionalConstructorArg(), new ParseField("end_offset"));
                PARSER.declareInt(optionalConstructorArg(), new ParseField("position"));
                PARSER.declareString(optionalConstructorArg(), new ParseField("payload"));
            }

            @Nullable
            private final Integer startOffset;
            @Nullable
            private final Integer endOffset;
            @Nullable
            private final Integer position;
            @Nullable
            private final String payload;

            public Token(Integer startOffset, Integer endOffset, Integer position, String payload) {
                this.startOffset = startOffset;
                this.endOffset = endOffset;
                this.position = position;
                this.payload = payload;
            }

            public static Token fromXContent(XContentParser parser) {
                return PARSER.apply(parser, null);
            }

            /**
             * Returns the start offset of the token in the document's field
             */
            public Integer getStartOffset() {
                return startOffset;
            }

            /**
             * Returns the end offset of the token in the document's field
             */
            public Integer getEndOffset() {
                return endOffset;
            }

            /**
             * Returns the position of the token in the document's field
             */
            public Integer getPosition() {
                return position;
            }

            /**
             * Returns the payload of the token or <code>null</code> if the payload doesn't exist
             */
            public String getPayload() {
                return payload;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) return true;
                if ((obj instanceof Token) == false) return false;
                Token other = (Token) obj;
                return Objects.equals(startOffset, other.startOffset)
                    && Objects.equals(endOffset, other.endOffset)
                    && Objects.equals(position, other.position)
                    && Objects.equals(payload, other.payload);
            }

            @Override
            public int hashCode() {
                return Objects.hash(startOffset, endOffset, position, payload);
            }
        }
    }
}
