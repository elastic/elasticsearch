/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.suggest;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.*;

/**
 * Top level suggest result, containing the result for each suggestion.
 */
public class Suggest implements Iterable<Suggest.Suggestion>, Streamable, ToXContent {

    static class Fields {

        static final XContentBuilderString SUGGEST = new XContentBuilderString("suggest");

    }

    private List<Suggestion> suggestions;

    Suggest() {
    }

    public Suggest(List<Suggestion> suggestions) {
        this.suggestions = suggestions;
    }

    /**
     * @return the suggestions
     */
    public List<Suggestion> getSuggestions() {
        return suggestions;
    }

    @Override
    public Iterator<Suggestion> iterator() {
        return suggestions.iterator();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        suggestions = new ArrayList<Suggestion>(size);
        for (int i = 0; i < size; i++) {
            Suggestion suggestion = new Suggestion();
            suggestion.readFrom(in);
            suggestions.add(suggestion);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(suggestions.size());
        for (Suggestion command : suggestions) {
            command.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SUGGEST);
        for (Suggestion suggestion : suggestions) {
            suggestion.toXContent(builder, params);
        }
        builder.endObject();
        return null;
    }

    public static Suggest readSuggest(StreamInput in) throws IOException {
        Suggest result = new Suggest();
        result.readFrom(in);
        return result;
    }

    /**
     * The suggestion responses corresponding with the suggestions in the request.
     */
    public static class Suggestion implements Streamable, ToXContent {

        static class Fields {

            static final XContentBuilderString TERMS = new XContentBuilderString("terms");

        }

        private String name;
        private int size;
        private Sort sort;
        private final List<Term> terms = new ArrayList<Term>(5);

        Suggestion() {
        }

        Suggestion(String name, int size, Sort sort) {
            this.name = name;
            this.size = size; // The suggested term size specified in request, only used for merging shard responses
            this.sort = sort;
        }

        void addTerm(Term term) {
            terms.add(term);
        }

        /**
         * @return The terms outputted by the suggest analyzer using the suggested text. Embeds the actual suggested
         *         terms.
         */
        public List<Term> getTerms() {
            return terms;
        }

        /**
         * @return The name of the suggestion as is defined in the request.
         */
        public String getName() {
            return name;
        }

        /**
         * Merges the result of another suggestion into this suggestion.
         */
        public void reduce(Suggestion other) {
            assert name.equals(other.name);
            assert terms.size() == other.terms.size();
            for (int i = 0; i < terms.size(); i++) {
                Term thisTerm = terms.get(i);
                Term otherTerm = other.terms.get(i);
                thisTerm.reduce(otherTerm, sort);
            }
        }

        /**
         * Trims the number of suggestions per suggest text term to the requested size.
         */
        public void trim() {
            for (Term term : terms) {
                term.trim(size);
            }
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            size = in.readVInt();
            sort = Sort.fromId(in.readByte());
            int size = in.readVInt();
            terms.clear();
            for (int i = 0; i < size; i++) {
                terms.add(Term.read(in));
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVInt(size);
            out.writeByte(sort.id());
            out.writeVInt(terms.size());
            for (Term term : terms) {
                term.writeTo(out);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(name);
            builder.startArray(Fields.TERMS);
            for (Term term : terms) {
                term.toXContent(builder, params);
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }


        /**
         * Represents a term from the suggest text, that contains the term, start/end offsets and zero or more suggested
         * terms for this term in the suggested text.
         */
        public static class Term implements Streamable, ToXContent {

            static class Fields {

                static final XContentBuilderString TERM = new XContentBuilderString("term");
                static final XContentBuilderString SUGGESTIONS = new XContentBuilderString("suggestions");
                static final XContentBuilderString START_OFFSET = new XContentBuilderString("start_offset");
                static final XContentBuilderString END_OFFSET = new XContentBuilderString("end_offset");

            }

            private Text term;
            private int startOffset;
            private int endOffset;

            private List<SuggestedTerm> suggested;

            public Term(Text term, int startOffset, int endOffset) {
                this.term = term;
                this.startOffset = startOffset;
                this.endOffset = endOffset;
                this.suggested = new ArrayList<SuggestedTerm>(5);
            }

            Term() {
            }

            void addSuggested(SuggestedTerm suggestedTerm) {
                suggested.add(suggestedTerm);
            }

            void reduce(Term otherTerm, Sort sort) {
                assert term.equals(otherTerm.term());
                assert startOffset == otherTerm.startOffset;
                assert endOffset == otherTerm.endOffset;

                for (SuggestedTerm otherSuggestedTerm : otherTerm.suggested) {
                    int index = suggested.indexOf(otherSuggestedTerm);
                    if (index >= 0) {
                        SuggestedTerm thisSuggestedTerm = suggested.get(index);
                        thisSuggestedTerm.setFrequency(thisSuggestedTerm.frequency + otherSuggestedTerm.frequency);
                    } else {
                        suggested.add(otherSuggestedTerm);
                    }
                }

                Comparator<SuggestedTerm> comparator;
                switch (sort) {
                    case SCORE:
                        comparator = SuggestPhase.SCORE;
                        break;
                    case FREQUENCY:
                        comparator = SuggestPhase.FREQUENCY;
                        break;
                    default:
                        throw new ElasticSearchException("Could not resolve comparator in reduce phase.");
                }
                Collections.sort(suggested, comparator);
            }

            public Text term() {
                return term;
            }

            /**
             * @return the term (analyzed by suggest analyzer) originating from the suggest text.
             */
            public String getTerm() {
                return term().string();
            }

            /**
             * @return the start offset of this term in the suggest text.
             */
            public int getStartOffset() {
                return startOffset;
            }

            /**
             * @return the end offset of this term in the suggest text.
             */
            public int getEndOffset() {
                return endOffset;
            }

            /**
             * @return The suggested terms for this particular suggest text term. If there are no suggested terms then
             *         an empty list is returned.
             */
            public List<SuggestedTerm> getSuggested() {
                return suggested;
            }

            void trim(int size) {
                int suggestionsToRemove = Math.max(0, suggested.size() - size);
                for (int i = 0; i < suggestionsToRemove; i++) {
                    suggested.remove(suggested.size() - 1);
                }
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Term term = (Term) o;

                if (endOffset != term.endOffset) return false;
                if (startOffset != term.startOffset) return false;
                if (!this.term.equals(term.term)) return false;

                return true;
            }

            @Override
            public int hashCode() {
                int result = term.hashCode();
                result = 31 * result + startOffset;
                result = 31 * result + endOffset;
                return result;
            }

            static Term read(StreamInput in) throws IOException {
                Term term = new Term();
                term.readFrom(in);
                return term;
            }

            @Override
            public void readFrom(StreamInput in) throws IOException {
                term = in.readText();
                startOffset = in.readVInt();
                endOffset = in.readVInt();
                int suggestedWords = in.readVInt();
                suggested = new ArrayList<SuggestedTerm>(suggestedWords);
                for (int j = 0; j < suggestedWords; j++) {
                    suggested.add(SuggestedTerm.create(in));
                }
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeText(term);
                out.writeVInt(startOffset);
                out.writeVInt(endOffset);
                out.writeVInt(suggested.size());
                for (SuggestedTerm suggestedTerm : suggested) {
                    suggestedTerm.writeTo(out);
                }
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(Fields.TERM, term);
                builder.field(Fields.START_OFFSET, startOffset);
                builder.field(Fields.END_OFFSET, endOffset);
                builder.startArray(Fields.SUGGESTIONS);
                for (SuggestedTerm suggestedTerm : suggested) {
                    suggestedTerm.toXContent(builder, params);
                }
                builder.endArray();
                builder.endObject();
                return builder;
            }

            /**
             * Represents the suggested term, containing a term and its document frequency and score.
             */
            public static class SuggestedTerm implements Streamable, ToXContent {

                static class Fields {

                    static final XContentBuilderString TERM = new XContentBuilderString("term");
                    static final XContentBuilderString FREQUENCY = new XContentBuilderString("frequency");
                    static final XContentBuilderString SCORE = new XContentBuilderString("score");

                }

                private Text term;
                private int frequency;
                private float score;

                SuggestedTerm(Text term, int frequency, float score) {
                    this.term = term;
                    this.frequency = frequency;
                    this.score = score;
                }

                SuggestedTerm() {
                }

                public void setFrequency(int frequency) {
                    this.frequency = frequency;
                }

                /**
                 * @return The actual term.
                 */
                public Text getTerm() {
                    return term;
                }

                /**
                 * @return How often this suggested term appears in the index.
                 */
                public int getFrequency() {
                    return frequency;
                }

                /**
                 * @return The score based on the edit distance difference between the suggested term and the
                 *         term in the suggest text.
                 */
                public float getScore() {
                    return score;
                }

                static SuggestedTerm create(StreamInput in) throws IOException {
                    SuggestedTerm suggestion = new SuggestedTerm();
                    suggestion.readFrom(in);
                    return suggestion;
                }

                @Override
                public void readFrom(StreamInput in) throws IOException {
                    term = in.readText();
                    frequency = in.readVInt();
                    score = in.readFloat();
                }

                @Override
                public void writeTo(StreamOutput out) throws IOException {
                    out.writeText(term);
                    out.writeVInt(frequency);
                    out.writeFloat(score);
                }

                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    builder.startObject();
                    builder.field(Fields.TERM, term);
                    builder.field(Fields.FREQUENCY, frequency);
                    builder.field(Fields.SCORE, score);
                    builder.endObject();
                    return builder;
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;

                    SuggestedTerm that = (SuggestedTerm) o;
                    return term.equals(that.term);

                }

                @Override
                public int hashCode() {
                    return term.hashCode();
                }
            }

        }

        enum Sort {

            /**
             * Sort should first be based on score.
             */
            SCORE((byte) 0x0),

            /**
             * Sort should first be based on document frequency.
             */
            FREQUENCY((byte) 0x1);

            private byte id;

            private Sort(byte id) {
                this.id = id;
            }

            public byte id() {
                return id;
            }

            static Sort fromId(byte id) {
                if (id == 0) {
                    return SCORE;
                } else if (id == 1) {
                    return FREQUENCY;
                } else {
                    throw new ElasticSearchException("Illegal suggest sort " + id);
                }
            }

        }

    }

}
