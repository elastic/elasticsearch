/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.suggest.term;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.suggest.SortBy;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Comparator;
import java.util.Objects;

/**
 * The suggestion responses corresponding with the suggestions in the request.
 */
public class TermSuggestion extends Suggestion<TermSuggestion.Entry> {

    public static final Comparator<Suggestion.Entry.Option> SCORE = new Score();
    public static final Comparator<Suggestion.Entry.Option> FREQUENCY = new Frequency();

    private final SortBy sort;

    public TermSuggestion(String name, int size, SortBy sort) {
        super(name, size);
        this.sort = sort;
    }

    public TermSuggestion(StreamInput in) throws IOException {
        super(in);
        sort = SortBy.readFromStream(in);
    }

    // Same behaviour as comparators in suggest module, but for SuggestedWord
    // Highest score first, then highest freq first, then lowest term first
    public static class Score implements Comparator<Suggestion.Entry.Option> {

        @Override
        public int compare(Suggestion.Entry.Option first, Suggestion.Entry.Option second) {
            // first criteria: the distance
            int cmp = Float.compare(second.getScore(), first.getScore());
            if (cmp != 0) {
                return cmp;
            }
            return FREQUENCY.compare(first, second);
        }
    }

    // Same behaviour as comparators in suggest module, but for SuggestedWord
    // Highest freq first, then highest score first, then lowest term first
    public static class Frequency implements Comparator<Suggestion.Entry.Option> {

        @Override
        public int compare(Suggestion.Entry.Option first, Suggestion.Entry.Option second) {

            // first criteria: the popularity
            int cmp = ((TermSuggestion.Entry.Option) second).getFreq() - ((TermSuggestion.Entry.Option) first).getFreq();
            if (cmp != 0) {
                return cmp;
            }

            // second criteria (if first criteria is equal): the distance
            cmp = Float.compare(second.getScore(), first.getScore());
            if (cmp != 0) {
                return cmp;
            }

            // third criteria: term text
            return first.getText().compareTo(second.getText());
        }
    }

    @Override
    protected Comparator<Option> sortComparator() {
        return switch (sort) {
            case SCORE -> SCORE;
            case FREQUENCY -> FREQUENCY;
        };
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        sort.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return TermSuggestionBuilder.SUGGESTION_NAME;
    }

    @Override
    protected Entry newEntry(StreamInput in) throws IOException {
        return new Entry(in);
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other) && Objects.equals(sort, ((TermSuggestion) other).sort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sort);
    }

    /**
     * Represents a part from the suggest text with suggested options.
     */
    public static class Entry extends Suggest.Suggestion.Entry<TermSuggestion.Entry.Option> {

        public Entry(Text text, int offset, int length) {
            super(text, offset, length);
        }

        public Entry() {}

        public Entry(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        protected Option newOption(StreamInput in) throws IOException {
            return new Option(in);
        }

        /**
         * Contains the suggested text with its document frequency and score.
         */
        public static class Option extends Suggest.Suggestion.Entry.Option {

            public static final ParseField FREQ = new ParseField("freq");

            private int freq;

            public Option(Text text, int freq, float score) {
                super(text, score);
                this.freq = freq;
            }

            public Option(StreamInput in) throws IOException {
                super(in);
                freq = in.readVInt();
            }

            @Override
            protected void mergeInto(Suggestion.Entry.Option otherOption) {
                super.mergeInto(otherOption);
                freq += ((Option) otherOption).freq;
            }

            /**
             * @return How often this suggested text appears in the index.
             */
            public int getFreq() {
                return freq;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeVInt(freq);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder = super.toXContent(builder, params);
                builder.field(FREQ.getPreferredName(), freq);
                return builder;
            }
        }
    }
}
