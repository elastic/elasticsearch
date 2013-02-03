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
    public static class Suggestion implements Iterable<Suggestion.Entry>, Streamable, ToXContent {

        private String name;
        private int size;
        private Sort sort;
        private final List<Entry> entries = new ArrayList<Entry>(5);

        Suggestion() {
        }

        Suggestion(String name, int size, Sort sort) {
            this.name = name;
            this.size = size; // The suggested term size specified in request, only used for merging shard responses
            this.sort = sort;
        }

        void addTerm(Entry entry) {
            entries.add(entry);
        }

        @Override
        public Iterator<Entry> iterator() {
            return entries.iterator();
        }

        /**
         * @return The entries for this suggestion.
         */
        public List<Entry> getEntries() {
            return entries;
        }

        /**
         * @return The name of the suggestion as is defined in the request.
         */
        public String getName() {
            return name;
        }

        /**
         * Merges the result of another suggestion into this suggestion.
         * For internal usage.
         */
        public void reduce(Suggestion other) {
            assert name.equals(other.name);
            assert entries.size() == other.entries.size();
            for (int i = 0; i < entries.size(); i++) {
                Entry thisEntry = entries.get(i);
                Entry otherEntry = other.entries.get(i);
                thisEntry.reduce(otherEntry, sort);
            }
        }

        /**
         * Trims the number of options per suggest text term to the requested size.
         * For internal usage.
         */
        public void trim() {
            for (Entry entry : entries) {
                entry.trim(size);
            }
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            size = in.readVInt();
            sort = Sort.fromId(in.readByte());
            int size = in.readVInt();
            entries.clear();
            for (int i = 0; i < size; i++) {
                entries.add(Entry.read(in));
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVInt(size);
            out.writeByte(sort.id());
            out.writeVInt(entries.size());
            for (Entry entry : entries) {
                entry.writeTo(out);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray(name);
            for (Entry entry : entries) {
                entry.toXContent(builder, params);
            }
            builder.endArray();
            return builder;
        }


        /**
         * Represents a part from the suggest text with suggested options.
         */
        public static class Entry implements Iterable<Entry.Option>, Streamable, ToXContent {

            static class Fields {

                static final XContentBuilderString TEXT = new XContentBuilderString("text");
                static final XContentBuilderString OFFSET = new XContentBuilderString("offset");
                static final XContentBuilderString LENGTH = new XContentBuilderString("length");
                static final XContentBuilderString OPTIONS = new XContentBuilderString("options");

            }

            private Text text;
            private int offset;
            private int length;

            private List<Option> options;

            Entry(Text text, int offset, int length) {
                this.text = text;
                this.offset = offset;
                this.length = length;
                this.options = new ArrayList<Option>(5);
            }

            Entry() {
            }

            void addOption(Option option) {
                options.add(option);
            }

            void reduce(Entry otherEntry, Sort sort) {
                assert text.equals(otherEntry.text);
                assert offset == otherEntry.offset;
                assert length == otherEntry.length;

                for (Option otherOption : otherEntry.options) {
                    int index = options.indexOf(otherOption);
                    if (index >= 0) {
                        Option thisOption = options.get(index);
                        thisOption.setFreq(thisOption.freq + otherOption.freq);
                    } else {
                        options.add(otherOption);
                    }
                }

                Comparator<Option> comparator;
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
                Collections.sort(options, comparator);
            }

            /**
             * @return the text (analyzed by suggest analyzer) originating from the suggest text. Usually this is a
             *         single term.
             */
            public Text getText() {
                return text;
            }

            /**
             * @return the start offset (not analyzed) for this entry in the suggest text.
             */
            public int getOffset() {
                return offset;
            }

            /**
             * @return the length (not analyzed) for this entry in the suggest text.
             */
            public int getLength() {
                return length;
            }

            @Override
            public Iterator<Option> iterator() {
                return options.iterator();
            }

            /**
             * @return The suggested options for this particular suggest entry. If there are no suggested terms then
             *         an empty list is returned.
             */
            public List<Option> getOptions() {
                return options;
            }

            void trim(int size) {
                int optionsToRemove = Math.max(0, options.size() - size);
                for (int i = 0; i < optionsToRemove; i++) {
                    options.remove(options.size() - 1);
                }
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Entry entry = (Entry) o;

                if (length != entry.length) return false;
                if (offset != entry.offset) return false;
                if (!this.text.equals(entry.text)) return false;

                return true;
            }

            @Override
            public int hashCode() {
                int result = text.hashCode();
                result = 31 * result + offset;
                result = 31 * result + length;
                return result;
            }

            static Entry read(StreamInput in) throws IOException {
                Entry entry = new Entry();
                entry.readFrom(in);
                return entry;
            }

            @Override
            public void readFrom(StreamInput in) throws IOException {
                text = in.readText();
                offset = in.readVInt();
                length = in.readVInt();
                int suggestedWords = in.readVInt();
                options = new ArrayList<Option>(suggestedWords);
                for (int j = 0; j < suggestedWords; j++) {
                    options.add(Option.create(in));
                }
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeText(text);
                out.writeVInt(offset);
                out.writeVInt(length);
                out.writeVInt(options.size());
                for (Option option : options) {
                    option.writeTo(out);
                }
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(Fields.TEXT, text);
                builder.field(Fields.OFFSET, offset);
                builder.field(Fields.LENGTH, length);
                builder.startArray(Fields.OPTIONS);
                for (Option option : options) {
                    option.toXContent(builder, params);
                }
                builder.endArray();
                builder.endObject();
                return builder;
            }

            /**
             * Contains the suggested text with its document frequency and score.
             */
            public static class Option implements Streamable, ToXContent {

                static class Fields {

                    static final XContentBuilderString TEXT = new XContentBuilderString("text");
                    static final XContentBuilderString FREQ = new XContentBuilderString("freq");
                    static final XContentBuilderString SCORE = new XContentBuilderString("score");

                }

                private Text text;
                private int freq;
                private float score;

                Option(Text text, int freq, float score) {
                    this.text = text;
                    this.freq = freq;
                    this.score = score;
                }

                Option() {
                }

                public void setFreq(int freq) {
                    this.freq = freq;
                }

                /**
                 * @return The actual suggested text.
                 */
                public Text getText() {
                    return text;
                }

                /**
                 * @return How often this suggested text appears in the index.
                 */
                public int getFreq() {
                    return freq;
                }

                /**
                 * @return The score based on the edit distance difference between the suggested term and the
                 *         term in the suggest text.
                 */
                public float getScore() {
                    return score;
                }

                static Option create(StreamInput in) throws IOException {
                    Option suggestion = new Option();
                    suggestion.readFrom(in);
                    return suggestion;
                }

                @Override
                public void readFrom(StreamInput in) throws IOException {
                    text = in.readText();
                    freq = in.readVInt();
                    score = in.readFloat();
                }

                @Override
                public void writeTo(StreamOutput out) throws IOException {
                    out.writeText(text);
                    out.writeVInt(freq);
                    out.writeFloat(score);
                }

                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    builder.startObject();
                    builder.field(Fields.TEXT, text);
                    builder.field(Fields.FREQ, freq);
                    builder.field(Fields.SCORE, score);
                    builder.endObject();
                    return builder;
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;

                    Option that = (Option) o;
                    return text.equals(that.text);

                }

                @Override
                public int hashCode() {
                    return text.hashCode();
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
