/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.suggest;

import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Top level suggest result, containing the result for each suggestion.
 */
public class Suggest implements Iterable<Suggest.Suggestion<? extends Entry<? extends Option>>>, Writeable, ToXContentFragment {

    public static final String NAME = "suggest";

    public static final Comparator<Option> COMPARATOR = (first, second) -> {
        int cmp = Float.compare(second.getScore(), first.getScore());
        if (cmp != 0) {
            return cmp;
        }
        return first.getText().compareTo(second.getText());
    };

    private final List<Suggestion<? extends Entry<? extends Option>>> suggestions;
    private final boolean hasScoreDocs;

    private Map<String, Suggestion<? extends Entry<? extends Option>>> suggestMap;

    public Suggest(List<Suggestion<? extends Entry<? extends Option>>> suggestions) {
        // we sort suggestions by their names to ensure iteration over suggestions are consistent
        // this is needed as we need to fill in suggestion docs in SearchPhaseController#sortDocs
        // in the same order as we enrich the suggestions with fetch results in SearchPhaseController#merge
        suggestions.sort((o1, o2) -> o1.getName().compareTo(o2.getName()));
        this.suggestions = suggestions;
        this.hasScoreDocs = filter(CompletionSuggestion.class).stream().anyMatch(CompletionSuggestion::hasScoreDocs);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Suggest(StreamInput in) throws IOException {
        suggestions = (List) in.readNamedWriteableList(Suggestion.class);
        hasScoreDocs = filter(CompletionSuggestion.class).stream().anyMatch(CompletionSuggestion::hasScoreDocs);
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

    @SuppressWarnings("unchecked")
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

    /**
     * Whether any suggestions had query hits
     */
    public boolean hasScoreDocs() {
        return hasScoreDocs;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableList(suggestions);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        for (Suggestion<?> suggestion : suggestions) {
            suggestion.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    /**
     * this parsing method assumes that the leading "suggest" field name has already been parsed by the caller
     */
    public static Suggest fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        List<Suggestion<? extends Entry<? extends Option>>> suggestions = new ArrayList<>();
        while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
            String currentField = parser.currentName();
            ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser);
            Suggestion<? extends Entry<? extends Option>> suggestion = Suggestion.fromXContent(parser);
            if (suggestion != null) {
                suggestions.add(suggestion);
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    String.format(Locale.ROOT, "Could not parse suggestion keyed as [%s]", currentField)
                );
            }
        }
        return new Suggest(suggestions);
    }

    public static List<Suggestion<? extends Entry<? extends Option>>> reduce(Map<String, List<Suggest.Suggestion<?>>> groupedSuggestions) {
        List<Suggestion<? extends Entry<? extends Option>>> reduced = new ArrayList<>(groupedSuggestions.size());
        for (Map.Entry<String, List<Suggestion<?>>> unmergedResults : groupedSuggestions.entrySet()) {
            List<Suggestion<?>> value = unmergedResults.getValue();
            @SuppressWarnings("rawtypes")
            Class<? extends Suggestion> suggestionClass = null;
            for (Suggestion<?> suggestion : value) {
                if (suggestionClass == null) {
                    suggestionClass = suggestion.getClass();
                } else if (suggestionClass != suggestion.getClass()) {
                    throw new IllegalArgumentException(
                        "detected mixed suggestion results, due to querying on old and new completion suggester,"
                            + " query on a single completion suggester version"
                    );
                }
            }
            @SuppressWarnings({ "unchecked", "rawtypes" })
            Suggestion<? extends Entry<? extends Option>> reduce = value.get(0).reduce((List) value);
            reduce.trim();
            reduced.add(reduce);
        }
        return reduced;
    }

    /**
     * @return only suggestions of type <code>suggestionType</code> contained in this {@link Suggest} instance
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T extends Suggestion> List<T> filter(Class<T> suggestionType) {
        return suggestions.stream()
            .filter(suggestion -> suggestion.getClass() == suggestionType)
            .map(suggestion -> (T) suggestion)
            .toList();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        return Objects.equals(suggestions, ((Suggest) other).suggestions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(suggestions);
    }

    /**
     * The suggestion responses corresponding with the suggestions in the request.
     */
    @SuppressWarnings("rawtypes")
    public abstract static class Suggestion<T extends Suggestion.Entry> implements Iterable<T>, NamedWriteable, ToXContentFragment {

        public static final int TYPE = 0;
        protected final String name;
        protected final int size;
        protected final List<T> entries = new ArrayList<>(5);

        public Suggestion(String name, int size) {
            this.name = name;
            this.size = size; // The suggested term size specified in request, only used for merging shard responses
        }

        public Suggestion(StreamInput in) throws IOException {
            name = in.readString();
            size = in.readVInt();

            int entriesCount = in.readVInt();
            entries.clear();
            for (int i = 0; i < entriesCount; i++) {
                T newEntry = newEntry(in);
                entries.add(newEntry);
            }
        }

        public void addTerm(T entry) {
            entries.add(entry);
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
         * @return The number of requested suggestion option size
         */
        public int getSize() {
            return size;
        }

        /**
         * Merges the result of another suggestion into this suggestion.
         * For internal usage.
         */
        @SuppressWarnings("unchecked")
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
                    if (suggestion.entries.size() != size) {
                        throw new IllegalStateException(
                            "Can't merge suggest result, this might be caused by suggest calls "
                                + "across multiple indices with different analysis chains. Suggest entries have different sizes actual ["
                                + suggestion.entries.size()
                                + "] expected ["
                                + size
                                + "]"
                        );
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

        protected abstract T newEntry(StreamInput in) throws IOException;

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVInt(size);
            out.writeList(entries);
        }

        @Override
        public abstract String getWriteableName();

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (params.paramAsBoolean(RestSearchAction.TYPED_KEYS_PARAM, false)) {
                // Concatenates the type and the name of the suggestion (ex: completion#foo)
                builder.startArray(String.join(Aggregation.TYPED_KEYS_DELIMITER, getWriteableName(), getName()));
            } else {
                builder.startArray(getName());
            }
            for (Entry<?> entry : entries) {
                builder.startObject();
                entry.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
            return builder;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            Suggestion otherSuggestion = (Suggestion) other;
            return Objects.equals(name, otherSuggestion.name)
                && Objects.equals(size, otherSuggestion.size)
                && Objects.equals(entries, otherSuggestion.entries);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, size, entries);
        }

        @SuppressWarnings("unchecked")
        public static Suggestion<? extends Entry<? extends Option>> fromXContent(XContentParser parser) throws IOException {
            ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
            SetOnce<Suggestion> suggestion = new SetOnce<>();
            XContentParserUtils.parseTypedKeysObject(parser, Aggregation.TYPED_KEYS_DELIMITER, Suggestion.class, suggestion::set);
            return suggestion.get();
        }

        protected static <E extends Suggestion.Entry<?>> void parseEntries(
            XContentParser parser,
            Suggestion<E> suggestion,
            CheckedFunction<XContentParser, E, IOException> entryParser
        ) throws IOException {
            ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
            while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                suggestion.addTerm(entryParser.apply(parser));
            }
        }

        /**
         * Represents a part from the suggest text with suggested options.
         */
        public abstract static class Entry<O extends Option> implements Iterable<O>, Writeable, ToXContentFragment {

            private static final String TEXT = "text";
            private static final String OFFSET = "offset";
            private static final String LENGTH = "length";
            protected static final String OPTIONS = "options";

            protected Text text;
            protected int offset;
            protected int length;

            protected List<O> options = new ArrayList<>(5);

            public Entry(Text text, int offset, int length) {
                this.text = text;
                this.offset = offset;
                this.length = length;
            }

            protected Entry() {}

            public Entry(StreamInput in) throws IOException {
                text = in.readText();
                offset = in.readVInt();
                length = in.readVInt();
                int suggestedWords = in.readVInt();
                options = new ArrayList<>(suggestedWords);
                for (int j = 0; j < suggestedWords; j++) {
                    O newOption = newOption(in);
                    options.add(newOption);
                }
            }

            public void addOption(O option) {
                options.add(option);
            }

            protected void addOptions(List<O> options) {
                for (O option : options) {
                    addOption(option);
                }
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
                    if (leader.text.equals(entry.text) == false) {
                        throw new IllegalStateException(
                            "Can't merge suggest entries, this might be caused by suggest calls "
                                + "across multiple indices with different analysis chains. Suggest entries have different text actual ["
                                + entry.text
                                + "] expected ["
                                + leader.text
                                + "]"
                        );
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
                for (O option : entries.keySet()) {
                    leader.addOption(option);
                }
                return leader;
            }

            /**
             * Merge any extra fields for this subtype.
             */
            protected void merge(Entry<O> other) {}

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
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }

                Entry<?> entry = (Entry<?>) o;
                return Objects.equals(length, entry.length)
                    && Objects.equals(offset, entry.offset)
                    && Objects.equals(text, entry.text)
                    && Objects.equals(options, entry.options);
            }

            @Override
            public int hashCode() {
                return Objects.hash(text, offset, length, options);
            }

            protected abstract O newOption(StreamInput in) throws IOException;

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeText(text);
                out.writeVInt(offset);
                out.writeVInt(length);
                out.writeCollection(options);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.field(TEXT, text);
                builder.field(OFFSET, offset);
                builder.field(LENGTH, length);
                builder.startArray(OPTIONS);
                for (Option option : options) {
                    builder.startObject();
                    option.toXContent(builder, params);
                    builder.endObject();
                }
                builder.endArray();
                return builder;
            }

            protected static void declareCommonFields(ObjectParser<? extends Entry<? extends Option>, Void> parser) {
                parser.declareString((entry, text) -> entry.text = new Text(text), new ParseField(TEXT));
                parser.declareInt((entry, offset) -> entry.offset = offset, new ParseField(OFFSET));
                parser.declareInt((entry, length) -> entry.length = length, new ParseField(LENGTH));
            }

            /**
             * Contains the suggested text with its document frequency and score.
             */
            public abstract static class Option implements Writeable, ToXContentFragment {

                public static final ParseField TEXT = new ParseField("text");
                public static final ParseField HIGHLIGHTED = new ParseField("highlighted");
                public static final ParseField SCORE = new ParseField("score");
                public static final ParseField COLLATE_MATCH = new ParseField("collate_match");

                private final Text text;
                private final Text highlighted;
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

                public Option(StreamInput in) throws IOException {
                    text = in.readText();
                    score = in.readFloat();
                    highlighted = in.readOptionalText();
                    collateMatch = in.readOptionalBoolean();
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
                public void writeTo(StreamOutput out) throws IOException {
                    out.writeText(text);
                    out.writeFloat(score);
                    out.writeOptionalText(highlighted);
                    out.writeOptionalBoolean(collateMatch);
                }

                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    builder.field(TEXT.getPreferredName(), text);
                    if (highlighted != null) {
                        builder.field(HIGHLIGHTED.getPreferredName(), highlighted);
                    }

                    builder.field(SCORE.getPreferredName(), score);
                    if (collateMatch != null) {
                        builder.field(COLLATE_MATCH.getPreferredName(), collateMatch.booleanValue());
                    }

                    return builder;
                }

                protected void mergeInto(Option otherOption) {
                    score = Math.max(score, otherOption.score);
                    if (otherOption.collateMatch != null) {
                        if (collateMatch == null) {
                            collateMatch = otherOption.collateMatch;
                        } else {
                            collateMatch |= otherOption.collateMatch;
                        }
                    }
                }

                /*
                 * We consider options equal if they have the same text, even if their other fields may differ
                 */
                @Override
                public boolean equals(Object o) {
                    if (this == o) {
                        return true;
                    }
                    if (o == null || getClass() != o.getClass()) {
                        return false;
                    }

                    Option that = (Option) o;
                    return Objects.equals(text, that.text);
                }

                @Override
                public int hashCode() {
                    return Objects.hash(text);
                }
            }
        }
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
