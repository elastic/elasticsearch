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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class CustomSuggester extends Suggester<CustomSuggester.CustomSuggestionsContext> {

    public static final CustomSuggester INSTANCE = new CustomSuggester();

    // This is a pretty dumb implementation which returns the original text + fieldName + custom config option + 12 or 123
    @Override
    public Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> innerExecute(
            String name,
            CustomSuggestionsContext suggestion,
            IndexSearcher searcher,
            CharsRefBuilder spare) throws IOException {

        // Get the suggestion context
        String text = suggestion.getText().utf8ToString();

        // create two suggestions with 12 and 123 appended
        CustomSuggestion response = new CustomSuggestion(name, suggestion.getSize(), "suggestion-dummy-value");

        CustomSuggestion.Entry entry = new CustomSuggestion.Entry(new Text(text), 0, text.length(), "entry-dummy-value");

        String firstOption =
            String.format(Locale.ROOT, "%s-%s-%s-%s", text, suggestion.getField(), suggestion.options.get("suffix"), "12");
        CustomSuggestion.Entry.Option option12 = new CustomSuggestion.Entry.Option(new Text(firstOption), 0.1f, "option-dummy-value-1");
        entry.addOption(option12);

        String secondOption =
            String.format(Locale.ROOT, "%s-%s-%s-%s", text, suggestion.getField(), suggestion.options.get("suffix"), "123");
        CustomSuggestion.Entry.Option option123 = new CustomSuggestion.Entry.Option(new Text(secondOption), 0.2f, "option-dummy-value-2");
        entry.addOption(option123);

        response.addTerm(entry);

        return response;
    }

    public static class CustomSuggestion extends Suggest.Suggestion<CustomSuggestion.Entry> {

        // todo xcontent

        public static final int TYPE = 999;

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
            return CustomSuggesterSearchIT.CustomSuggestionBuilder.SUGGESTION_NAME;
        }

        @Override
        public int getWriteableType() {
            return TYPE;
        }

        /**
         * A meaningless value used to test that plugin suggesters can add fields to their Suggestion types
         */
        public String getDummy() {
            return dummy;
        }

        @Override
        protected Entry newEntry() {
            return new Entry();
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
                PARSER.declareString((entry, dummy) -> entry.dummy = dummy, new ParseField("dummy"));
                PARSER.declareObjectArray(Entry::addOptions, (p, c) -> Option.fromXContent(p), new ParseField(OPTIONS));
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
            protected Option newOption() {
                return new Option();
            }

            @Override
            protected Option newOption(StreamInput in) throws IOException {
                return new Option(in);
            }

            /**
             * Meaningless field used to test that plugin suggesters can add fields to their entries
             */
            public String getDummy() {
                return dummy;
            }

            public static Entry fromXContent(XContentParser parser) {
                return PARSER.apply(parser, null);
            }

            public static class Option extends Suggest.Suggestion.Entry.Option {

                public static final ParseField DUMMY = new ParseField("dummy");

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

                public Option() {}

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

                @Override
                protected void mergeInto(Suggest.Suggestion.Entry.Option otherOption) {
                    super.mergeInto(otherOption);
                    dummy += ((Option) otherOption).getDummy();
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

    public static class CustomSuggestionsContext extends SuggestionSearchContext.SuggestionContext {

        public Map<String, Object> options;

        public CustomSuggestionsContext(QueryShardContext context, Map<String, Object> options) {
            super(new CustomSuggester(), context);
            this.options = options;
        }
    }
}
