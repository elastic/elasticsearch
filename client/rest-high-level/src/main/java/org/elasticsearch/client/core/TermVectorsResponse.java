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

package org.elasticsearch.client.core;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Comparator;
import java.util.Objects;

public class TermVectorsResponse extends ActionResponse implements ToXContentObject {
    private final String index;
    private final String type;
    private final String id;
    private final long docVersion;
    private final boolean found;
    private final long tookInMillis;
    private final List<TermVector> termVectorList;

    public TermVectorsResponse(
        String index, String type, String id, long version, boolean found, long tookInMillis, List<TermVector> termVectorList) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.docVersion = version;
        this.found = found;
        this.tookInMillis = tookInMillis;
        if (termVectorList != null) {
            Collections.sort(termVectorList, Comparator.comparing(TermVector::getFieldName));
        }
        this.termVectorList = termVectorList;
    }

    private static ConstructingObjectParser<TermVectorsResponse, Void> PARSER = new ConstructingObjectParser<>(
        "term_vectors", true, args  -> new TermVectorsResponse((String) args[0], (String) args[1],
        (String) args[2], (long) args[3], (boolean) args[4], (long) args[5], (List<TermVector>) args[6]));

    static {
        PARSER.declareString(constructorArg(), new ParseField("_index"));
        PARSER.declareString(constructorArg(), new ParseField("_type"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("_id"));
        PARSER.declareLong(constructorArg(), new ParseField("_version"));
        PARSER.declareBoolean(constructorArg(), new ParseField("found"));
        PARSER.declareLong(constructorArg(), new ParseField("took"));
        PARSER.declareNamedObjects(optionalConstructorArg(),
            (p, c, fieldName) -> TermVector.fromXContent(p, fieldName), new ParseField("term_vectors"));
    }

    public static TermVectorsResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("_index", index);
        builder.field("_type", type);
        if (id != null) {
            builder.field("_id", id);
        }
        builder.field("_version", docVersion);
        builder.field("found", found);
        builder.field("took", tookInMillis);
        if (termVectorList != null) {
            builder.startObject("term_vectors");
            buildFromTermVectorList(builder);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    private void buildFromTermVectorList(XContentBuilder builder) throws IOException {
        for (TermVector tv : termVectorList) {
            builder.startObject(tv.fieldName);
            // build fields_statistics
            if (tv.fieldStatistics != null) {
                builder.startObject("field_statistics");
                builder.field("sum_doc_freq", tv.fieldStatistics.sumDocFreq);
                builder.field("doc_count", tv.fieldStatistics.docCount);
                builder.field("sum_ttf", tv.fieldStatistics.sumTotalTermFreq);
                builder.endObject();
            }
            // build terms
            if (tv.terms != null) {
                builder.startObject("terms");
                for (TermVector.Term term : tv.terms) {
                    builder.startObject(term.term);
                    // build term_statistics
                    if (term.docFreq > -1) builder.field("doc_freq", term.docFreq);
                    if (term.totalTermFreq > -1) builder.field("ttf", term.totalTermFreq);

                    builder.field("term_freq", term.termFreq);
                    // build tokens
                    if (term.tokens != null) {
                        builder.startArray("tokens");
                        for (TermVector.Token token : term.tokens) {
                            builder.startObject();
                            if (token.position > -1) builder.field("position", token.position);
                            if (token.startOffset > -1) builder.field("start_offset", token.startOffset);
                            if (token.endOffset > -1) builder.field("end_offset", token.endOffset);
                            if (token.payload != null) builder.field("payload", token.payload);
                            builder.endObject();
                        }
                        builder.endArray();
                    }
                    if (term.score > -1) builder.field("score", term.score);
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public Boolean getFound() {
        return found;
    }

    public List<TermVector> getTermVectorsList(){
        return termVectorList;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof TermVectorsResponse)) return false;
        TermVectorsResponse other = (TermVectorsResponse) obj;
        return index.equals(other.index)
            && type.equals(other.type)
            && Objects.equals(id, other.id)
            && docVersion == other.docVersion
            && found == other.found
            && tookInMillis == tookInMillis
            && Objects.equals(termVectorList, other.termVectorList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, type, id, docVersion, found, tookInMillis, termVectorList);
    }


    public static final class TermVector {

        private static ConstructingObjectParser<TermVector, String> TVPARSER = new ConstructingObjectParser<>(
            "term_vector", true, (args, ctxFieldName)  -> new TermVector( ctxFieldName, (FieldStatistics) args[0], (List<Term>) args[2]));

        static {
            TVPARSER.declareObject(optionalConstructorArg(),
                (p,c) -> FieldStatistics.fromXContent(p), new ParseField("field_statistics"));
            TVPARSER.declareNamedObjects(optionalConstructorArg(), (p, c, term) -> Term.fromXContent(p, term), new ParseField("terms"));
        }

        private final String fieldName;
        @Nullable
        private final FieldStatistics fieldStatistics;
        @Nullable
        private final List<Term> terms;

        public TermVector(String fieldName, FieldStatistics fieldStatistics, List<Term> terms) {
            this.fieldName = fieldName;
            this.fieldStatistics = fieldStatistics;
            if (terms != null) {
                Collections.sort(terms, Comparator.comparing(Term::getTerm));
            }
            this.terms = terms;
        }

        public static TermVector fromXContent(XContentParser parser, String fieldName) {
            return TVPARSER.apply(parser, null);
        }

        public String getFieldName() {
            return fieldName;
        }

        public List<Term> getTerms() {
            return terms;
        }

        public FieldStatistics getFieldStatistics() {
            return fieldStatistics;
        }


        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof TermVector)) return false;
            TermVector other = (TermVector) obj;
            return fieldName.equals(other.fieldName)
                && Objects.equals(fieldStatistics, other.fieldStatistics)
                && Objects.equals(terms, other.terms);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, fieldStatistics, terms);
        }

        public static final class FieldStatistics {

            private static ConstructingObjectParser<FieldStatistics, Void> FSPARSER = new ConstructingObjectParser<>(
                "field_statistics", true, args  -> new FieldStatistics((long) args[0], (int) args[1], (long) args[2]));

            static {
                FSPARSER.declareLong(constructorArg(), new ParseField("sum_doc_freq"));
                FSPARSER.declareInt(constructorArg(), new ParseField("doc_count"));
                FSPARSER.declareLong(constructorArg(), new ParseField("sum_ttf"));
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
                return FSPARSER.apply(parser, null);
            }

            public int getDocCount() {
                return docCount;
            }
            public long getSumDocFreq() {
                return sumDocFreq;
            }
            public long getSumTotalTermFreq() {
                return sumTotalTermFreq;
            }
            @Override
            public boolean equals(Object obj) {
                if (this == obj) return true;
                if (!(obj instanceof FieldStatistics)) return false;
                FieldStatistics other = (FieldStatistics) obj;
                return docCount == other.docCount
                    && sumDocFreq == other.sumDocFreq
                    && sumTotalTermFreq == other.sumTotalTermFreq;
            }

            @Override
            public int hashCode() {
                return Objects.hash(docCount, sumDocFreq, sumTotalTermFreq);
            }
        }


        public static final class Term {

            private static ConstructingObjectParser<Term, String> TERMPARSER = new ConstructingObjectParser<>(
                "token", true, (args, ctxTerm)  -> new Term( ctxTerm, (int) args[0],
                (Integer) args[1], (Long) args[2], (Float) args[3], (List<Token>) args[4]));
            static {
                TERMPARSER.declareInt(constructorArg(), new ParseField("term_freq"));
                TERMPARSER.declareInt(optionalConstructorArg(), new ParseField("doc_freq"));
                TERMPARSER.declareLong(optionalConstructorArg(), new ParseField("ttf"));
                TERMPARSER.declareFloat(optionalConstructorArg(), new ParseField("score"));
                TERMPARSER.declareObjectArray(optionalConstructorArg(), (p,c) -> Token.fromXContent(p), new ParseField("tokens"));
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
                if (tokens != null) {
                    Collections.sort(tokens,
                        Comparator.comparing(Token::getPosition).thenComparing(Token::getStartOffset).thenComparing(Token::getEndOffset));
                }
                this.tokens = tokens;
            }

            public static Term fromXContent(XContentParser parser, String term) {
                return TERMPARSER.apply(parser, term);
            }

            public String getTerm() {
                return term;
            }

            public int getTermFreq() {
                return termFreq;
            }

            public Integer getDocFreq() {
                return docFreq;
            }

            public Long getTotalTermFreq( ){
                return totalTermFreq;
            }

            public Float getScore(){
                return score;
            }

            public List<Token> getTokens() {
                return tokens;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) return true;
                if (!(obj instanceof Term)) return false;
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

            private static ConstructingObjectParser<Token, Void> TOKENPARSER = new ConstructingObjectParser<>(
                "token", true, args  -> new Token((Integer) args[0], (Integer) args[1],
                (Integer) args[2], (String) args[3]));
            static {
                TOKENPARSER.declareInt(optionalConstructorArg(), new ParseField("start_offset"));
                TOKENPARSER.declareInt(optionalConstructorArg(), new ParseField("end_offset"));
                TOKENPARSER.declareInt(optionalConstructorArg(), new ParseField("position"));
                TOKENPARSER.declareString(optionalConstructorArg(), new ParseField("payload"));
            }

            @Nullable
            private final Integer startOffset;
            @Nullable
            private final Integer endOffset;
            @Nullable
            private final Integer position;
            @Nullable
            private final String payload;


            public Token(Integer startOffset, Integer endOffset, Integer position,  String payload) {
                this.startOffset = startOffset;
                this.endOffset = endOffset;
                this.position = position;
                this.payload = payload;
            }

            public static Token fromXContent(XContentParser parser) {
                return TOKENPARSER.apply(parser, null);
            }

            public Integer getStartOffset() {
                return startOffset;
            }

            public Integer getEndOffset() {
                return endOffset;
            }

            public Integer getPosition() {
                return position;
            }

            public String getPayload() {
                return payload;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) return true;
                if (!(obj instanceof Token)) return false;
                Token other = (Token) obj;
                return Objects.equals(startOffset, other.startOffset)
                    && Objects.equals(endOffset,other.endOffset)
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
