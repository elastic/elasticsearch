/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.example.customsuggester;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class CustomSuggestion extends Suggest.Suggestion<CustomSuggestion.Entry> {

    public static final ParseField DUMMY = new ParseField("dummy");

    private String dummy;

    public CustomSuggestion(String name, int size, String dummy) {
        super(name, size);
        this.dummy = dummy;
    }

    public CustomSuggestion(StreamInput in) throws IOException {
        super(in);
        dummy = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(dummy);
    }

    @Override
    public String getWriteableName() {
        return CustomSuggestionBuilder.SUGGESTION_NAME;
    }

    /**
     * A meaningless value used to test that plugin suggesters can add fields to their Suggestion types
     *
     * This can't be serialized to xcontent because Suggestions appear in xcontent as an array of entries, so there is no place
     * to add a custom field. But we can still use a custom field internally and use it to define a Suggestion's behavior
     */
    public String getDummy() {
        return dummy;
    }

    @Override
    protected Entry newEntry(StreamInput in) throws IOException {
        return new Entry(in);
    }

    public static class Entry extends Suggest.Suggestion.Entry<CustomSuggestion.Entry.Option> {

        private String dummy;

        public Entry() {}

        public Entry(Text text, int offset, int length, String dummy) {
            super(text, offset, length);
            this.dummy = dummy;
        }

        public Entry(StreamInput in) throws IOException {
            super(in);
            dummy = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(dummy);
        }

        @Override
        protected Option newOption(StreamInput in) throws IOException {
            return new Option(in);
        }

        /*
         * the value of dummy will always be the same, so this just tests that we can merge entries with custom fields
         */
        @Override
        protected void merge(Suggest.Suggestion.Entry<Option> otherEntry) {
            dummy = ((Entry) otherEntry).getDummy();
        }

        /**
         * Meaningless field used to test that plugin suggesters can add fields to their entries
         */
        public String getDummy() {
            return dummy;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder = super.toXContent(builder, params);
            builder.field(DUMMY.getPreferredName(), getDummy());
            return builder;
        }

        public static class Option extends Suggest.Suggestion.Entry.Option {

            private String dummy;

            public Option(Text text, float score, String dummy) {
                super(text, score);
                this.dummy = dummy;
            }

            public Option(StreamInput in) throws IOException {
                super(in);
                dummy = in.readString();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeString(dummy);
            }

            /**
             * A meaningless value used to test that plugin suggesters can add fields to their options
             */
            public String getDummy() {
                return dummy;
            }

            /*
             * the value of dummy will always be the same, so this just tests that we can merge options with custom fields
             */
            @Override
            protected void mergeInto(Suggest.Suggestion.Entry.Option otherOption) {
                super.mergeInto(otherOption);
                dummy = ((Option) otherOption).getDummy();
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder = super.toXContent(builder, params);
                builder.field(DUMMY.getPreferredName(), dummy);
                return builder;
            }
        }
    }
}
