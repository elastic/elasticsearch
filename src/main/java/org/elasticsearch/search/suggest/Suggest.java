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
package org.elasticsearch.search.suggest;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestion;

import java.io.IOException;
import java.util.*;

/**
 * Top level suggest result, containing the result for each suggestion.
 */
public class Suggest implements Iterable<Suggest.Suggestion<? extends Entry<? extends Option>>>, Streamable, ToXContent {

    public static class Fields {
        public static final XContentBuilderString SUGGEST = new XContentBuilderString("suggest");
    }

    private static final Comparator<Option> COMPARATOR = new Comparator<Suggest.Suggestion.Entry.Option>() {
        @Override
        public int compare(Option first, Option second) {
            int cmp = Float.compare(second.getScore(), first.getScore());
            if (cmp != 0) {
                return cmp;
            }
            return first.getText().compareTo(second.getText());
         }
    };

    private final XContentBuilderString name;

    private List<Suggestion<? extends Entry<? extends Option>>> suggestions;

    private Map<String, Suggestion<? extends Entry<? extends Option>>> suggestMap;

    public Suggest() {
        this.name = null;
    }

    public Suggest(XContentBuilderString name) {
        this.name = name;
    }

    public Suggest(List<Suggestion<? extends Entry<? extends Option>>> suggestions) {
        this(null, suggestions);
    }

    public Suggest(XContentBuilderString name, List<Suggestion<? extends Entry<? extends Option>>> suggestions) {
        this.name = name;
        this.suggestions = suggestions;
    }
    
    @Override
    public Iterator<Suggestion<? extends Entry<? extends Option>>> iterator() {
        return suggestions.iterator();
    }
    
    /**
     * The number of suggestions in this {@link Suggest} result
     */
    public int size() {
        return suggestions.size();
    }
    
    public <T extends Suggestion<? extends Entry<? extends Option>>> T getSuggestion(String name) {
        if (suggestions.isEmpty() || name == null) {
            return null;
        } else if (suggestions.size() == 1) {
          return (T) (name.equals(suggestions.get(0).name) ? suggestions.get(0) : null);
        } else if (this.suggestMap == null) {
            suggestMap = new HashMap<>();
            for (Suggest.Suggestion<? extends Entry<? extends Option>> item : suggestions) {
                suggestMap.put(item.getName(), item);
            }
        }
        return (T) suggestMap.get(name);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        final int size = in.readVInt();
        suggestions = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Suggestion<? extends Entry<? extends Option>> suggestion;
            final int type = in.readVInt();
            switch (type) {
            case TermSuggestion.TYPE:
                suggestion = new TermSuggestion();
                break;
            case CompletionSuggestion.TYPE:
                suggestion = new CompletionSuggestion();
                break;
            case PhraseSuggestion.TYPE:
                suggestion = new PhraseSuggestion();
                break;
            default:
                suggestion = new Suggestion<>();
                break;
            }
            suggestion.readFrom(in);
            suggestions.add(suggestion);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(suggestions.size());
        for (Suggestion<?> command : suggestions) {
            out.writeVInt(command.getType());
            command.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if(name == null) {
            for (Suggestion<?> suggestion : suggestions) {
                suggestion.toXContent(builder, params);
            }
        } else {
            builder.startObject(name);
            for (Suggestion<?> suggestion : suggestions) {
                suggestion.toXContent(builder, params);
            }
            builder.endObject();
        }
        
        return builder;
    }

    public static Suggest readSuggest(XContentBuilderString name, StreamInput in) throws IOException {
        Suggest result = new Suggest(name);
        result.readFrom(in);
        return result;
    }
    
    public static Map<String, List<Suggest.Suggestion>> group(Map<String, List<Suggest.Suggestion>> groupedSuggestions, Suggest suggest) {
        for (Suggestion<? extends Entry<? extends Option>> suggestion : suggest) {
            List<Suggestion> list = groupedSuggestions.get(suggestion.getName());
            if (list == null) {
                list = new ArrayList<>();
                groupedSuggestions.put(suggestion.getName(), list);
            }
            list.add(suggestion);
        }
        return groupedSuggestions;
    }

    public static List<Suggestion<? extends Entry<? extends Option>>> reduce(Map<String, List<Suggest.Suggestion>> groupedSuggestions) {
        List<Suggestion<? extends Entry<? extends Option>>> reduced = new ArrayList<>(groupedSuggestions.size());
        for (java.util.Map.Entry<String, List<Suggestion>> unmergedResults : groupedSuggestions.entrySet()) {
            List<Suggestion> value = unmergedResults.getValue();
            Suggestion reduce = value.get(0).reduce(value);
            reduce.trim();
            reduced.add(reduce);
        }
        return reduced;
    }

    /**
     * The suggestion responses corresponding with the suggestions in the request.
     */
    public static class Suggestion<T extends Suggestion.Entry> implements Iterable<T>, Streamable, ToXContent {
        
        
        public static final int TYPE = 0;
        protected String name;
        protected int size;
        protected final List<T> entries = new ArrayList<>(5);

        public Suggestion() {
        }

        public Suggestion(String name, int size) {
            this.name = name;
            this.size = size; // The suggested term size specified in request, only used for merging shard responses
        }

        public void addTerm(T entry) {
            entries.add(entry);
        }
        
        public int getType() {
            return TYPE;
        }

        @Override
        public Iterator<T> iterator() {
            return entries.iterator();
        }

        /**
         * @return The entries for this suggestion.
         */
        public List<T> getEntries() {
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
        public Suggestion<T> reduce(List<Suggestion<T>> toReduce) {
            if (toReduce.size() == 1) {
                return toReduce.get(0);
            } else if (toReduce.isEmpty()) {
                return null;
            }
            Suggestion<T> leader = toReduce.get(0);
            List<T> entries = leader.entries;
            final int size = entries.size();
            Comparator<Option> sortComparator = sortComparator();
            List<T> currentEntries = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                for (Suggestion<T> suggestion : toReduce) {
                    if(suggestion.entries.size() != size) {
                        throw new ElasticsearchIllegalStateException("Can't merge suggest result, this might be caused by suggest calls " +
                                "across multiple indices with different analysis chains. Suggest entries have different sizes actual [" +
                                suggestion.entries.size() + "] expected [" + size +"]");
                    }
                    assert suggestion.name.equals(leader.name);
                    currentEntries.add(suggestion.entries.get(i));
                }
                T entry = (T) entries.get(i).reduce(currentEntries);
                entry.sort(sortComparator);
                entries.set(i, entry);
                currentEntries.clear();
            }
            return leader;
        }
        
        protected Comparator<Option> sortComparator() {
            return COMPARATOR;
        }
        
        /**
         * Trims the number of options per suggest text term to the requested size.
         * For internal usage.
         */
        public void trim() {
            for (Entry<?> entry : entries) {
                entry.trim(size);
            }
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            innerReadFrom(in);
            int size = in.readVInt();
            entries.clear();
            for (int i = 0; i < size; i++) {
                T newEntry = newEntry();
                newEntry.readFrom(in);
                entries.add(newEntry);
            }
        }
        
        protected T newEntry() {
            return (T)new Entry();
        }

        
        protected void innerReadFrom(StreamInput in) throws IOException {
            name = in.readString();
            size = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            innerWriteTo(out);
            out.writeVInt(entries.size());
            for (Entry<?> entry : entries) {
                entry.writeTo(out);
            }
        }

        public void innerWriteTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVInt(size);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray(name);
            for (Entry<?> entry : entries) {
                entry.toXContent(builder, params);
            }
            builder.endArray();
            return builder;
        }


        /**
         * Represents a part from the suggest text with suggested options.
         */
        public static class Entry<O extends Entry.Option> implements Iterable<O>, Streamable, ToXContent {

            static class Fields {

                static final XContentBuilderString TEXT = new XContentBuilderString("text");
                static final XContentBuilderString OFFSET = new XContentBuilderString("offset");
                static final XContentBuilderString LENGTH = new XContentBuilderString("length");
                static final XContentBuilderString OPTIONS = new XContentBuilderString("options");

            }

            protected Text text;
            protected int offset;
            protected int length;

            protected List<O> options;

            public Entry(Text text, int offset, int length) {
                this.text = text;
                this.offset = offset;
                this.length = length;
                this.options = new ArrayList<>(5);
            }

            public Entry() {
            }

            public void addOption(O option) {
                options.add(option);
            }
            
            protected void sort(Comparator<O> comparator) {
                CollectionUtil.timSort(options, comparator);
            }

            protected <T extends Entry<O>> Entry<O> reduce(List<T> toReduce) {
                if (toReduce.size() == 1) {
                    return toReduce.get(0);
                }
                final Map<O, O> entries = new HashMap<>();
                Entry<O> leader = toReduce.get(0);
                for (Entry<O> entry : toReduce) {
                    if (!leader.text.equals(entry.text)) {
                        throw new ElasticsearchIllegalStateException("Can't merge suggest entries, this might be caused by suggest calls " +
                                "across multiple indices with different analysis chains. Suggest entries have different text actual [" +
                                entry.text + "] expected [" + leader.text +"]");
                    }
                    assert leader.offset == entry.offset;
                    assert leader.length == entry.length;
                    leader.merge(entry);
                    for (O option : entry) {
                        O merger = entries.get(option);
                        if (merger == null) {
                            entries.put(option, option);
                        } else {
                            merger.mergeInto(option);
                        }
                    }
                }
                leader.options.clear();
                for (O option: entries.keySet()) {
                    leader.addOption(option);
                }
                return leader;
            }

            /**
             * Merge any extra fields for this subtype.
             */
            protected void merge(Entry<O> other) {
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
            public Iterator<O> iterator() {
                return options.iterator();
            }

            /**
             * @return The suggested options for this particular suggest entry. If there are no suggested terms then
             *         an empty list is returned.
             */
            public List<O> getOptions() {
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

                Entry<?> entry = (Entry<?>) o;

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

            @Override
            public void readFrom(StreamInput in) throws IOException {
                text = in.readText();
                offset = in.readVInt();
                length = in.readVInt();
                int suggestedWords = in.readVInt();
                options = new ArrayList<>(suggestedWords);
                for (int j = 0; j < suggestedWords; j++) {
                    O newOption = newOption();
                    newOption.readFrom(in);
                    options.add(newOption);
                }
            }
            
            protected O newOption(){
                return (O) new Option();
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
                    static final XContentBuilderString HIGHLIGHTED = new XContentBuilderString("highlighted");
                    static final XContentBuilderString SCORE = new XContentBuilderString("score");
                    static final XContentBuilderString COLLATE_MATCH = new XContentBuilderString("collate_match");

                }

                private Text text;
                private Text highlighted;
                private float score;
                private Boolean collateMatch;

                public Option(Text text, Text highlighted, float score, Boolean collateMatch) {
                    this.text = text;
                    this.highlighted = highlighted;
                    this.score = score;
                    this.collateMatch = collateMatch;
                }

                public Option(Text text, Text highlighted, float score) {
                    this(text, highlighted, score, null);
                }

                public Option(Text text, float score) {
                    this(text, null, score);
                }

                public Option() {
                }

                /**
                 * @return The actual suggested text.
                 */
                public Text getText() {
                    return text;
                }

                /**
                 * @return Copy of suggested text with changes from user supplied text highlighted.
                 */
                public Text getHighlighted() {
                    return highlighted;
                }

                /**
                 * @return The score based on the edit distance difference between the suggested term and the
                 *         term in the suggest text.
                 */
                public float getScore() {
                    return score;
                }

                /**
                 * @return true if collation has found a match for the entry.
                 * if collate was not set, the value defaults to <code>true</code>
                 */
                public boolean collateMatch() {
                    return (collateMatch != null) ? collateMatch : true;
                }
                
                protected void setScore(float score) {
                    this.score = score;
                }

                @Override
                public void readFrom(StreamInput in) throws IOException {
                    text = in.readText();
                    score = in.readFloat();
                    highlighted = in.readOptionalText();
                    collateMatch = in.readOptionalBoolean();
                }

                @Override
                public void writeTo(StreamOutput out) throws IOException {
                    out.writeText(text);
                    out.writeFloat(score);
                    out.writeOptionalText(highlighted);
                    out.writeOptionalBoolean(collateMatch);
                }

                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    builder.startObject();
                    innerToXContent(builder, params);
                    builder.endObject();
                    return builder;
                }
                
                protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
                    builder.field(Fields.TEXT, text);
                    if (highlighted != null) {
                        builder.field(Fields.HIGHLIGHTED, highlighted);
                    }
                    builder.field(Fields.SCORE, score);
                    if (collateMatch != null) {
                        builder.field(Fields.COLLATE_MATCH, collateMatch.booleanValue());
                    }
                    return builder;
                }
                
                protected void mergeInto(Option otherOption) {
                    score = Math.max(score, otherOption.score);
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

        public enum Sort {

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

            public static Sort fromId(byte id) {
                if (id == 0) {
                    return SCORE;
                } else if (id == 1) {
                    return FREQUENCY;
                } else {
                    throw new ElasticsearchException("Illegal suggest sort " + id);
                }
            }
        }
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }
}
