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

package org.elasticsearch.example.customsuggester;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.suggest.Suggest;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class CustomSuggestion extends Suggest.Suggestion<CustomSuggestion.Entry> {

    public static final int TYPE = 999;

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

    @Override
    public int getWriteableType() {
        return TYPE;
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

    public static CustomSuggestion fromXContent(XContentParser parser, String name) throws IOException {
        CustomSuggestion suggestion = new CustomSuggestion(name, -1, null);
        parseEntries(parser, suggestion, Entry::fromXContent);
        return suggestion;
    }

    public static class Entry extends Suggest.Suggestion.Entry<CustomSuggestion.Entry.Option> {

        private static final ObjectParser<Entry, Void> PARSER = new ObjectParser<>("CustomSuggestionEntryParser", true, Entry::new);

        static {
            declareCommonFields(PARSER);
            PARSER.declareString((entry, dummy) -> entry.dummy = dummy, DUMMY);
            /*
             * The use of a lambda expression instead of the method reference Entry::addOptions is a workaround for a JDK 14 compiler bug.
             * The bug is: https://bugs.java.com/bugdatabase/view_bug.do?bug_id=JDK-8242214
             */
            PARSER.declareObjectArray((e, o) -> e.addOptions(o), (p, c) -> Option.fromXContent(p), new ParseField(OPTIONS));
        }

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

        public static Entry fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        public static class Option extends Suggest.Suggestion.Entry.Option {

            private static final ConstructingObjectParser<Option, Void> PARSER = new ConstructingObjectParser<>(
                "CustomSuggestionObjectParser", true,
                args -> {
                    Text text = new Text((String) args[0]);
                    float score = (float) args[1];
                    String dummy = (String) args[2];
                    return new Option(text, score, dummy);
                });

            static {
                PARSER.declareString(constructorArg(), TEXT);
                PARSER.declareFloat(constructorArg(), SCORE);
                PARSER.declareString(constructorArg(), DUMMY);
            }

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

            public static Option fromXContent(XContentParser parser) {
                return PARSER.apply(parser, null);
            }
        }
    }
}
