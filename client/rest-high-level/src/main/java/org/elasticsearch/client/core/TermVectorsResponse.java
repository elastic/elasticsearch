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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.ObjectParser;
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
    private List<TermVector> termVectorList = null;

    public TermVectorsResponse(String index, String type, String id, long version, boolean found, long tookInMillis) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.docVersion = version;
        this.found = found;
        this.tookInMillis = tookInMillis;
    }

    private static ConstructingObjectParser<TermVectorsResponse, Void> PARSER = new ConstructingObjectParser<>(
        "term_vectors", true, args  ->
        new TermVectorsResponse((String) args[0], (String) args[1], (String) args[2], (long) args[3], (boolean) args[4], (long) args[5]));

    static {
        PARSER.declareString(constructorArg(), new ParseField("_index"));
        PARSER.declareString(constructorArg(), new ParseField("_type"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("_id"));
        PARSER.declareLong(constructorArg(), new ParseField("_version"));
        PARSER.declareBoolean(constructorArg(), new ParseField("found"));
        PARSER.declareLong(constructorArg(), new ParseField("took"));
        PARSER.declareNamedObjects(TermVectorsResponse::setTermVectorsList, (p, c, fieldName) -> TermVector.fromXContent(p, fieldName),
            new ParseField("term_vectors"));
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

    public void setTermVectorsList(List<TermVector> termVectorList) {
        this.termVectorList = termVectorList;
        if (termVectorList != null) {
            Collections.sort(this.termVectorList, Comparator.comparing(TermVector::getFieldName));
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

        private static ObjectParser<TermVector, Void> TVPARSER =
            new ObjectParser<>(TermVector.class.getSimpleName(), true, TermVector::new);
        private static ConstructingObjectParser<FieldStatistics, Void> FSPARSER = new ConstructingObjectParser<>(
            "field_statistics", true, args  -> new FieldStatistics((long) args[0], (int) args[1], (long) args[2]));
        private static ObjectParser<Term, Void> TERMPARSER = new ObjectParser<>(Term.class.getSimpleName(), true, Term::new);
        private static ObjectParser<Token, Void> TOKENPARSER = new ObjectParser<>(Token.class.getSimpleName(), true, Token::new);
        static {
            TVPARSER.declareObject(TermVector::setFieldStatistics, FSPARSER, new ParseField("field_statistics"));
            TVPARSER.declareNamedObjects(TermVector::setTerms, (p, c, term) -> Term.fromXContent(p, term), new ParseField("terms"));

            FSPARSER.declareLong(constructorArg(), new ParseField("sum_doc_freq"));
            FSPARSER.declareInt(constructorArg(), new ParseField("doc_count"));
            FSPARSER.declareLong(constructorArg(), new ParseField("sum_ttf"));

            TERMPARSER.declareInt(Term::setTermFreq, new ParseField("term_freq"));
            TERMPARSER.declareInt(Term::setDocFreq, new ParseField("doc_freq"));
            TERMPARSER.declareLong(Term::setTotalTermFreq, new ParseField("ttf"));
            TERMPARSER.declareFloat(Term::setScore, new ParseField("score"));
            TERMPARSER.declareObjectArray(Term::setTokens, (p,c) -> Token.fromXContent(p), new ParseField("tokens"));

            TOKENPARSER.declareInt(Token::setPosition, new ParseField("position"));
            TOKENPARSER.declareInt(Token::setStartOffset, new ParseField("start_offset"));
            TOKENPARSER.declareInt(Token::setEndOffset, new ParseField("end_offset"));
            TOKENPARSER.declareString(Token::setPayload, new ParseField("payload"));
        }

        private String fieldName;
        private FieldStatistics fieldStatistics = null;
        private List<Term> terms = null;

        public TermVector() {}

        public static TermVector fromXContent(XContentParser parser, String fieldName) {
            TermVector tv = TVPARSER.apply(parser, null);
            tv.setFieldName(fieldName);
            return tv;
        }

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public List<Term> getTerms() {
            return terms;
        }

        public void setTerms(List<Term> terms) {
            this.terms = terms;
            if (terms != null) {
                Collections.sort(this.terms, Comparator.comparing(Term::getTerm));
            }
        }

        public FieldStatistics getFieldStatistics() {
            return fieldStatistics;
        }

        public void setFieldStatistics(FieldStatistics fieldStatistics) {
            this.fieldStatistics = fieldStatistics;
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
            private final long sumDocFreq;
            private final int docCount;
            private final long sumTotalTermFreq;

            public FieldStatistics(long sumDocFreq, int docCount, long sumTotalTermFreq) {
                this.sumDocFreq = sumDocFreq;
                this.docCount = docCount;
                this.sumTotalTermFreq = sumTotalTermFreq;
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
            private String term;
            private int termFreq = -1;
            private int docFreq = -1;
            private long totalTermFreq = -1;
            private float score = -1f;
            private List<Token> tokens = null;

            public Term() {};

            public static Term fromXContent(XContentParser parser, String term) {
                Term t = TERMPARSER.apply(parser, null);
                t.setTerm(term);
                return t;
            }

            public String getTerm() {
                return term;
            }

            public void setTerm(String term) {
                this.term = term;
            }

            public void setTermFreq(int termFreq) {
                this.termFreq = termFreq;
            }

            public int getTermFreq() {
                return termFreq;
            }

            public void setDocFreq(int docFreq) {
                this.docFreq = docFreq;
            }

            public int getDocFreq() {
                return docFreq;
            }

            public void setTotalTermFreq(long totalTermFreq) {
                this.totalTermFreq = totalTermFreq;
            }

            public long getTotalTermFreq( ){
                return totalTermFreq;
            }

            public void setScore(float score) {
                this.score = score;
            }

            public float getScore(){
                return score;
            }

            public void setTokens(List<Token> tokens) {
                this.tokens = tokens;
                if (tokens != null) {
                    Collections.sort(this.tokens,
                        Comparator.comparing(Token::getPosition).thenComparing(Token::getStartOffset).thenComparing(Token::getEndOffset));
                }
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
                    && docFreq == other.docFreq
                    && totalTermFreq == other.totalTermFreq
                    && (Float.compare(score, other.score) == 0)
                    && Objects.equals(tokens, other.tokens);
            }

            @Override
            public int hashCode() {
                return Objects.hash(term, termFreq, docFreq, totalTermFreq, score, tokens);
            }
        }

        public static final class Token {
            private int position = -1;
            private int startOffset = -1;
            private int endOffset = -1;
            private String payload = null;

            public Token() {};

            public static Token fromXContent(XContentParser parser) {
                Token t = TOKENPARSER.apply(parser, null);
                return t;
            }

            public int getPosition() {
                return position;
            }

            public void setPosition(int position) {
                this.position = position;
            }

            public int getStartOffset() {
                return startOffset;
            }

            public void setStartOffset(int startOffset) {
                this.startOffset = startOffset;
            }

            public int getEndOffset() {
                return endOffset;
            }

            public void setEndOffset(int endOffset) {
                this.endOffset = endOffset;
            }

            public String getPayload() {
                return payload;
            }

            public void setPayload(String payload) {
                this.payload = payload;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) return true;
                if (!(obj instanceof Token)) return false;
                Token other = (Token) obj;
                return startOffset == other.startOffset
                    && endOffset == other.endOffset
                    && position == other.position
                    && Objects.equals(payload, other.payload);
            }

            @Override
            public int hashCode() {
                return Objects.hash(startOffset, endOffset, position, payload);
            }

        }
        }
}
