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

package org.elasticsearch.action.termvectors;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BoostAttribute;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest.Flag;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.dfs.AggregatedDfs;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TermVectorsResponse extends ActionResponse implements ToXContentObject {

     static class FieldStrings {
        // term statistics strings
        public static final String TTF = "ttf";
        public static final String DOC_FREQ = "doc_freq";
        public static final String TERM_FREQ = "term_freq";
        public static final String SCORE = "score";

        // field statistics strings
        public static final String FIELD_STATISTICS = "field_statistics";
        public static final String DOC_COUNT = "doc_count";
        public static final String SUM_DOC_FREQ = "sum_doc_freq";
        public static final String SUM_TTF = "sum_ttf";

        public static final String TOKENS = "tokens";
        public static final String POS = "position";
        public static final String START_OFFSET = "start_offset";
        public static final String END_OFFSET = "end_offset";
        public static final String PAYLOAD = "payload";
        public static final String _INDEX = "_index";
        public static final String _TYPE = "_type";
        public static final String _ID = "_id";
        public static final String _VERSION = "_version";
        public static final String FOUND = "found";
        public static final String TOOK = "took";
        public static final String TERMS = "terms";
        public static final String TERM_VECTORS = "term_vectors";
    }

    private BytesReference termVectors;
    private BytesReference headerRef;
    private String index;
    private String type;
    private String id;
    private long docVersion;
    private boolean exists = false;
    private boolean artificial = false;
    private long tookInMillis;
    private boolean hasScores = false;
    private List<TermVector> termVectorList = null;

    private boolean sourceCopied = false;

    int[] currentPositions = new int[0];
    int[] currentStartOffset = new int[0];
    int[] currentEndOffset = new int[0];
    BytesReference[] currentPayloads = new BytesReference[0];

    public TermVectorsResponse(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    TermVectorsResponse() {
    }

    //constructor used for HLRC
    public TermVectorsResponse(String index, String type, String id, long version, boolean found, long tookInMillis) {
        this.index = index;
        this.type = type;
        if (id == null) {
            this.artificial = true;
        } else {
            this.id = id;
        }
        this.docVersion = version;
        this.exists = found;
        this.tookInMillis = tookInMillis;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeString(type);
        out.writeString(id);
        out.writeVLong(docVersion);
        final boolean docExists = isExists();
        out.writeBoolean(docExists);
        out.writeBoolean(artificial);
        out.writeVLong(tookInMillis);
        out.writeBoolean(hasTermVectors());
        if (hasTermVectors()) {
            if (out.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
                if (termVectorList == null) { //server's version
                    out.writeBoolean(true);
                    out.writeBytesReference(headerRef);
                    out.writeBytesReference(termVectors);
                } else { //HLRC's version - necessary only for tests
                    out.writeBoolean(false);
                    out.writeList(termVectorList);
                }
            } else {
                out.writeBytesReference(headerRef);
                out.writeBytesReference(termVectors);
            }
        }
    }

    public boolean hasTermVectors() {
        assert (headerRef == null && termVectors == null) || (headerRef != null && termVectors != null);
        return (headerRef != null) || (termVectorList != null);
    }

    private static ConstructingObjectParser<TermVectorsResponse, Void> PARSER = new ConstructingObjectParser<>("term_vectors", true,
            args  -> new TermVectorsResponse((String) args[0], (String) args[1], (String) args[2],
            (long) args[3], (boolean) args[4], (long) args[5]));
    static {
        PARSER.declareString(constructorArg(), new ParseField(FieldStrings._INDEX));
        PARSER.declareString(constructorArg(), new ParseField(FieldStrings._TYPE));
        PARSER.declareString(optionalConstructorArg(), new ParseField(FieldStrings._ID));
        PARSER.declareLong(constructorArg(), new ParseField(FieldStrings._VERSION));
        PARSER.declareBoolean(constructorArg(), new ParseField(FieldStrings.FOUND));
        PARSER.declareLong(constructorArg(), new ParseField(FieldStrings.TOOK));
        PARSER.declareNamedObjects(TermVectorsResponse::setTermVectorsList, (p, c, fieldName) -> TermVector.fromXContent(p, fieldName),
            new ParseField(FieldStrings.TERM_VECTORS));
    }

    // Deserializaton from the HLRC
    public static TermVectorsResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        index = in.readString();
        type = in.readString();
        id = in.readString();
        docVersion = in.readVLong();
        exists = in.readBoolean();
        artificial = in.readBoolean();
        tookInMillis = in.readVLong();
        if (in.readBoolean()) {
            if (in.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
                if (in.readBoolean()) { //server's version
                    headerRef = in.readBytesReference();
                    termVectors = in.readBytesReference();
                } else { //HLRC's version - necessary only for tests
                    termVectorList = in.readList(TermVector::new);
                }
            } else {
                headerRef = in.readBytesReference();
                termVectors = in.readBytesReference();
            }
        }
    }

    public Fields getFields() throws IOException {
        if (hasTermVectors() && isExists()) {
            if (!sourceCopied) { // make the bytes safe
                headerRef = new BytesArray(headerRef.toBytesRef(), true);
                termVectors = new BytesArray(termVectors.toBytesRef(), true);
            }
            TermVectorsFields termVectorsFields = new TermVectorsFields(headerRef, termVectors);
            hasScores = termVectorsFields.hasScores;
            return termVectorsFields;
        } else {
            return new Fields() {
                @Override
                public Iterator<String> iterator() {
                    return Collections.emptyIterator();
                }

                @Override
                public Terms terms(String field) throws IOException {
                    return null;
                }

                @Override
                public int size() {
                    return 0;
                }
            };
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        assert index != null;
        assert type != null;
        assert id != null;
        builder.startObject();
        builder.field(FieldStrings._INDEX, index);
        builder.field(FieldStrings._TYPE, type);
        if (!isArtificial()) {
            builder.field(FieldStrings._ID, id);
        }
        builder.field(FieldStrings._VERSION, docVersion);
        builder.field(FieldStrings.FOUND, isExists());
        builder.field(FieldStrings.TOOK, tookInMillis);
        if (isExists()) {
            builder.startObject(FieldStrings.TERM_VECTORS);
            if (termVectorList == null) { //server's version
                final CharsRefBuilder spare = new CharsRefBuilder();
                Fields theFields = getFields();
                Iterator<String> fieldIter = theFields.iterator();
                while (fieldIter.hasNext()) {
                    buildField(builder, spare, theFields, fieldIter);
                }
            } else { //HLRC's version
                buildFromTermVectorList(builder);
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof TermVectorsResponse)) return false;
        TermVectorsResponse other = (TermVectorsResponse) obj;
        return index.equals(other.index)
            && type.equals(other.type)
            && id.equals(other.id)
            && artificial == other.artificial
            && docVersion == other.docVersion
            && exists == other.exists
            && tookInMillis == other.tookInMillis
            && Objects.equals(termVectorList, other.termVectorList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, type, id, artificial, docVersion, exists, tookInMillis, termVectorList);
    }

    private void buildField(XContentBuilder builder, final CharsRefBuilder spare, Fields theFields, Iterator<String> fieldIter) throws IOException {
        String fieldName = fieldIter.next();
        builder.startObject(fieldName);
        Terms curTerms = theFields.terms(fieldName);
        // write field statistics
        buildFieldStatistics(builder, curTerms);
        builder.startObject(FieldStrings.TERMS);
        TermsEnum termIter = curTerms.iterator();
        BoostAttribute boostAtt = termIter.attributes().addAttribute(BoostAttribute.class);
        for (int i = 0; i < curTerms.size(); i++) {
            buildTerm(builder, spare, curTerms, termIter, boostAtt);
        }
        builder.endObject();
        builder.endObject();
    }

    private void buildTerm(XContentBuilder builder, final CharsRefBuilder spare, Terms curTerms, TermsEnum termIter, BoostAttribute boostAtt) throws IOException {
        // start term, optimized writing
        BytesRef term = termIter.next();
        spare.copyUTF8Bytes(term);
        builder.startObject(spare.toString());
        buildTermStatistics(builder, termIter);
        // finally write the term vectors
        PostingsEnum posEnum = termIter.postings(null, PostingsEnum.ALL);
        int termFreq = posEnum.freq();
        builder.field(FieldStrings.TERM_FREQ, termFreq);
        initMemory(curTerms, termFreq);
        initValues(curTerms, posEnum, termFreq);
        buildValues(builder, curTerms, termFreq);
        buildScore(builder, boostAtt);
        builder.endObject();
    }

    private void buildTermStatistics(XContentBuilder builder, TermsEnum termIter) throws IOException {
        // write term statistics. At this point we do not naturally have a
        // boolean that says if these values actually were requested.
        // However, we can assume that they were not if the statistic values are
        // <= 0.
        assert (((termIter.docFreq() > 0) && (termIter.totalTermFreq() > 0)) || ((termIter.docFreq() == -1) && (termIter.totalTermFreq() == -1)));
        int docFreq = termIter.docFreq();
        if (docFreq > 0) {
            builder.field(FieldStrings.DOC_FREQ, docFreq);
            builder.field(FieldStrings.TTF, termIter.totalTermFreq());
        }
    }

    private void buildValues(XContentBuilder builder, Terms curTerms, int termFreq) throws IOException {
        if (!(curTerms.hasPayloads() || curTerms.hasOffsets() || curTerms.hasPositions())) {
            return;
        }

        builder.startArray(FieldStrings.TOKENS);
        for (int i = 0; i < termFreq; i++) {
            builder.startObject();
            if (curTerms.hasPositions()) {
                builder.field(FieldStrings.POS, currentPositions[i]);
            }
            if (curTerms.hasOffsets()) {
                builder.field(FieldStrings.START_OFFSET, currentStartOffset[i]);
                builder.field(FieldStrings.END_OFFSET, currentEndOffset[i]);
            }
            if (curTerms.hasPayloads() && (currentPayloads[i].length() > 0)) {
                BytesRef bytesRef = currentPayloads[i].toBytesRef();
                builder.field(FieldStrings.PAYLOAD, bytesRef.bytes, bytesRef.offset, bytesRef.length);
            }
            builder.endObject();
        }
        builder.endArray();
    }

    private void initValues(Terms curTerms, PostingsEnum posEnum, int termFreq) throws IOException {
        for (int j = 0; j < termFreq; j++) {
            int nextPos = posEnum.nextPosition();
            if (curTerms.hasPositions()) {
                currentPositions[j] = nextPos;
            }
            if (curTerms.hasOffsets()) {
                currentStartOffset[j] = posEnum.startOffset();
                currentEndOffset[j] = posEnum.endOffset();
            }
            if (curTerms.hasPayloads()) {
                BytesRef curPayload = posEnum.getPayload();
                if (curPayload != null) {
                    currentPayloads[j] = new BytesArray(curPayload.bytes, 0, curPayload.length);
                } else {
                    currentPayloads[j] = null;
                }
            }
        }
    }

    private void initMemory(Terms curTerms, int termFreq) {
        // init memory for performance reasons
        if (curTerms.hasPositions()) {
            currentPositions = ArrayUtil.grow(currentPositions, termFreq);
        }
        if (curTerms.hasOffsets()) {
            currentStartOffset = ArrayUtil.grow(currentStartOffset, termFreq);
            currentEndOffset = ArrayUtil.grow(currentEndOffset, termFreq);
        }
        if (curTerms.hasPayloads()) {
            currentPayloads = new BytesArray[termFreq];
        }
    }

    private void buildFieldStatistics(XContentBuilder builder, Terms curTerms) throws IOException {
        long sumDocFreq = curTerms.getSumDocFreq();
        int docCount = curTerms.getDocCount();
        long sumTotalTermFrequencies = curTerms.getSumTotalTermFreq();
        if (docCount >= 0) {
            assert ((sumDocFreq >= 0)) : "docCount >= 0 but sumDocFreq ain't!";
            assert ((sumTotalTermFrequencies >= 0)) : "docCount >= 0 but sumTotalTermFrequencies ain't!";
            builder.startObject(FieldStrings.FIELD_STATISTICS);
            builder.field(FieldStrings.SUM_DOC_FREQ, sumDocFreq);
            builder.field(FieldStrings.DOC_COUNT, docCount);
            builder.field(FieldStrings.SUM_TTF, sumTotalTermFrequencies);
            builder.endObject();
        } else if (docCount == -1) { // this should only be -1 if the field
            // statistics were not requested at all. In
            // this case all 3 values should be -1
            assert ((sumDocFreq == -1)) : "docCount was -1 but sumDocFreq ain't!";
            assert ((sumTotalTermFrequencies == -1)) : "docCount was -1 but sumTotalTermFrequencies ain't!";
        } else {
            throw new IllegalStateException(
                    "Something is wrong with the field statistics of the term vector request: Values are " + "\n"
                            + FieldStrings.SUM_DOC_FREQ + " " + sumDocFreq + "\n" + FieldStrings.DOC_COUNT + " " + docCount + "\n"
                            + FieldStrings.SUM_TTF + " " + sumTotalTermFrequencies);
        }
    }

    private void buildFromTermVectorList(XContentBuilder builder) throws IOException {
        for (TermVector tv : termVectorList) {
            builder.startObject(tv.fieldName);
            // build fields_statistics
            if (tv.fieldStatistics != null) {
                builder.startObject(FieldStrings.FIELD_STATISTICS);
                builder.field(FieldStrings.SUM_DOC_FREQ, tv.fieldStatistics.sumDocFreq);
                builder.field(FieldStrings.DOC_COUNT, tv.fieldStatistics.docCount);
                builder.field(FieldStrings.SUM_TTF, tv.fieldStatistics.sumTotalTermFreq);
                builder.endObject();
            }
            // build terms
            if (tv.terms != null) {
                builder.startObject(FieldStrings.TERMS);
                for (TermVector.Term term : tv.terms) {
                    builder.startObject(term.term);
                    // build term_statistics
                    if (term.docFreq > -1) builder.field(FieldStrings.DOC_FREQ, term.docFreq);
                    if (term.totalTermFreq > -1) builder.field(FieldStrings.TTF, term.totalTermFreq);

                    builder.field(FieldStrings.TERM_FREQ, term.termFreq);
                    // build tokens
                    if (term.tokens != null) {
                        builder.startArray(FieldStrings.TOKENS);
                        for (TermVector.Token token : term.tokens) {
                            builder.startObject();
                            if (token.position > -1) builder.field(FieldStrings.POS, token.position);
                            if (token.startOffset > -1) builder.field(FieldStrings.START_OFFSET, token.startOffset);
                            if (token.endOffset > -1) builder.field(FieldStrings.END_OFFSET, token.endOffset);
                            if (token.payload != null) builder.field(FieldStrings.PAYLOAD, token.payload);
                            builder.endObject();
                        }
                        builder.endArray();
                    }
                    if (term.score > -1) builder.field(FieldStrings.SCORE, term.score);
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
    }

    public void setTookInMillis(long tookInMillis) {
        this.tookInMillis = tookInMillis;
    }

    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    private void buildScore(XContentBuilder builder, BoostAttribute boostAtt) throws IOException {
        if (hasScores) {
            builder.field(FieldStrings.SCORE, boostAtt.getBoost());
        }
    }

    public boolean isExists() {
        return exists;
    }

    public void setExists(boolean exists) {
         this.exists = exists;
    }

    public void setFields(Fields termVectorsByField, Set<String> selectedFields, EnumSet<Flag> flags, Fields topLevelFields) throws IOException {
        setFields(termVectorsByField, selectedFields, flags, topLevelFields, null, null);
    }

    public void setFields(Fields termVectorsByField, Set<String> selectedFields, EnumSet<Flag> flags, Fields topLevelFields, @Nullable AggregatedDfs dfs,
                          TermVectorsFilter termVectorsFilter) throws IOException {
        TermVectorsWriter tvw = new TermVectorsWriter(this);

        if (termVectorsByField != null) {
            tvw.setFields(termVectorsByField, selectedFields, flags, topLevelFields, dfs, termVectorsFilter);
        }
    }

    public void setTermVectorsField(BytesStreamOutput output) {
        termVectors = output.bytes();
    }

    public void setHeader(BytesReference header) {
        headerRef = header;
    }

    public void setDocVersion(long version) {
        this.docVersion = version;
    }

    public Long getVersion() {
        return docVersion;
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

    public boolean isArtificial() {
        return artificial;
    }

    public void setArtificial(boolean artificial) {
        this.artificial = artificial;
    }

    public void setTermVectorsList(List<TermVector> termVectorList){
        this.termVectorList = termVectorList;
        if (termVectorList != null) {
            Collections.sort(this.termVectorList, Comparator.comparing(TermVector::getFieldName));
        }
    }

    public List<TermVector> getTermVectorsList(){
        return termVectorList;
    }

    public static final class TermVector implements Writeable {

        private static ObjectParser<TermVector, Void> TVPARSER =
            new ObjectParser<>(TermVector.class.getSimpleName(), true, TermVector::new);
        private static ConstructingObjectParser<FieldStatistics, Void> FSPARSER = new ConstructingObjectParser<>("field_statistics", true,
            args  -> new FieldStatistics((long) args[0], (int) args[1], (long) args[2]));
        private static ObjectParser<Term, Void> TERMPARSER = new ObjectParser<>(Term.class.getSimpleName(), true, Term::new);
        private static ObjectParser<Token, Void> TOKENPARSER = new ObjectParser<>(Token.class.getSimpleName(), true, Token::new);
        static {
            TVPARSER.declareObject(TermVector::setFieldStatistics, FSPARSER, new ParseField(FieldStrings.FIELD_STATISTICS));
            TVPARSER.declareNamedObjects(TermVector::setTerms, (p, c, term) -> Term.fromXContent(p, term),
                new ParseField(FieldStrings.TERMS));

            FSPARSER.declareLong(constructorArg(), new ParseField(FieldStrings.SUM_DOC_FREQ));
            FSPARSER.declareInt(constructorArg(), new ParseField(FieldStrings.DOC_COUNT));
            FSPARSER.declareLong(constructorArg(), new ParseField(FieldStrings.SUM_TTF));

            TERMPARSER.declareInt(Term::setTermFreq, new ParseField(FieldStrings.TERM_FREQ));
            TERMPARSER.declareInt(Term::setDocFreq, new ParseField(FieldStrings.DOC_FREQ));
            TERMPARSER.declareLong(Term::setTotalTermFreq, new ParseField(FieldStrings.TTF));
            TERMPARSER.declareFloat(Term::setScore, new ParseField(FieldStrings.SCORE));
            TERMPARSER.declareObjectArray(Term::setTokens, (p,c) -> Token.fromXContent(p), new ParseField(FieldStrings.TOKENS));

            TOKENPARSER.declareInt(Token::setPosition, new ParseField(FieldStrings.POS));
            TOKENPARSER.declareInt(Token::setStartOffset, new ParseField(FieldStrings.START_OFFSET));
            TOKENPARSER.declareInt(Token::setEndOffset, new ParseField(FieldStrings.END_OFFSET));
            TOKENPARSER.declareString(Token::setPayload, new ParseField(FieldStrings.PAYLOAD));
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

        /** Serialization constructor. */
        public TermVector(StreamInput in) throws IOException {
            this.fieldName = in.readString();
            if (in.readBoolean()) {
                fieldStatistics = new FieldStatistics(in.readLong(), in.readInt(), in.readLong());
            }
            if (in.readBoolean()) {
                terms = in.readList(Term::new);
            }
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
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(fieldName);
            if (fieldStatistics != null){
                out.writeBoolean(true);
                out.writeLong(fieldStatistics.sumDocFreq);
                out.writeInt(fieldStatistics.docCount);
                out.writeLong(fieldStatistics.getSumTotalTermFreq());
            } else {
                out.writeBoolean(false);
            }
            if (terms != null) {
                out.writeBoolean(true);
                out.writeList(terms);
            } else {
                out.writeBoolean(false);
            }
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

        public static final class Term implements Writeable {
            private String term;
            private int termFreq = -1;
            private int docFreq = -1;
            private long totalTermFreq = -1;
            private float score = -1f;
            private List<Token> tokens = null;

            public Term() {};

            /** Serialization constructor. */
            public Term(StreamInput in) throws IOException {
                term = in.readString();
                termFreq = in.readInt();
                if (in.readBoolean()) {
                    docFreq = in.readInt();
                    totalTermFreq = in.readLong();
                }
                if (in.readBoolean()) {
                    score = in.readFloat();
                }
                if (in.readBoolean()) {
                    tokens = in.readList(Token::new);
                }
            }

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
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(term);
                out.writeInt(termFreq);
                if (docFreq > -1) {
                    out.writeBoolean(true);
                    out.writeInt(docFreq);
                    out.writeLong(totalTermFreq);
                } else {
                    out.writeBoolean(false);
                }
                if (score > -1f) {
                    out.writeBoolean(true);
                    out.writeFloat(score);
                } else {
                    out.writeBoolean(false);
                }
                if (tokens != null) {
                    out.writeBoolean(true);
                    out.writeList(tokens);
                } else {
                    out.writeBoolean(false);
                }
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

        public static final class Token implements Writeable {
            private int position = -1;
            private int startOffset = -1;
            private int endOffset = -1;
            private String payload = null;

            public Token() {};

            /** Serialization constructor. */
            public Token(StreamInput in) throws IOException {
                position = in.readInt();
                startOffset = in.readInt();
                endOffset = in.readInt();
                payload = in.readOptionalString();
            }

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
            public void writeTo(StreamOutput out) throws IOException {
                out.writeInt(position);
                out.writeInt(startOffset);
                out.writeInt(endOffset);
                out.writeOptionalString(payload);
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
