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

package org.elasticsearch.search.suggest.phrase;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.Suggest.Suggestion;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Suggestion entry returned from the {@link PhraseSuggester}.
 */
public class PhraseSuggestion extends Suggest.Suggestion<PhraseSuggestion.Entry> {

    @Deprecated
    public static final int TYPE = 3;

    public PhraseSuggestion(String name, int size) {
        super(name, size);
    }

    public PhraseSuggestion(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return PhraseSuggestionBuilder.SUGGESTION_NAME;
    }

    @Override
    public int getWriteableType() {
        return TYPE;
    }

    @Override
    protected Entry newEntry(StreamInput in) throws IOException {
        return new Entry(in);
    }

    public static PhraseSuggestion fromXContent(XContentParser parser, String name) throws IOException {
        PhraseSuggestion suggestion = new PhraseSuggestion(name, -1);
        parseEntries(parser, suggestion, PhraseSuggestion.Entry::fromXContent);
        return suggestion;
    }

    public static class Entry extends Suggestion.Entry<PhraseSuggestion.Entry.Option> {

        protected double cutoffScore = Double.MIN_VALUE;

        public Entry(Text text, int offset, int length, double cutoffScore) {
            super(text, offset, length);
            this.cutoffScore = cutoffScore;
        }

        public Entry(Text text, int offset, int length) {
            super(text, offset, length);
        }

        Entry() {}

        public Entry(StreamInput in) throws IOException {
            super(in);
            cutoffScore = in.readDouble();
        }

        /**
         * @return cutoff score for suggestions.  input term score * confidence for phrase suggest, 0 otherwise
         */
        public double getCutoffScore() {
            return cutoffScore;
        }

        @Override
        protected void merge(Suggestion.Entry<Option> other) {
            super.merge(other);
            // If the cluster contains both pre 0.90.4 and post 0.90.4 nodes then we'll see Suggestion.Entry
            // objects being merged with PhraseSuggestion.Entry objects.  We merge Suggestion.Entry objects
            // by assuming they had a low cutoff score rather than a high one as that is the more common scenario
            // and the simplest one for us to implement.
            if (!(other instanceof PhraseSuggestion.Entry)) {
                return;
            }
            PhraseSuggestion.Entry otherSuggestionEntry = (PhraseSuggestion.Entry) other;
            this.cutoffScore = Math.max(this.cutoffScore, otherSuggestionEntry.cutoffScore);
        }

        @Override
        public void addOption(Option option) {
            if (option.getScore() > this.cutoffScore) {
                this.options.add(option);
            }
        }

        private static ObjectParser<Entry, Void> PARSER = new ObjectParser<>("PhraseSuggestionEntryParser", true, Entry::new);

        static {
            declareCommonFields(PARSER);
            PARSER.declareObjectArray(Entry::addOptions, (p, c) -> Option.fromXContent(p), new ParseField(OPTIONS));
        }

        public static Entry fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        protected Option newOption(StreamInput in) throws IOException {
            return new Option(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeDouble(cutoffScore);
        }

        @Override
        public boolean equals(Object other) {
            return super.equals(other)
                && Objects.equals(cutoffScore, ((Entry) other).cutoffScore);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), cutoffScore);
        }

        public static class Option extends Suggestion.Entry.Option {

            public Option(Text text, Text highlighted, float score, Boolean collateMatch) {
                super(text, highlighted, score, collateMatch);
            }

            public Option(Text text, Text highlighted, float score) {
                super(text, highlighted, score);
            }

            public Option(StreamInput in) throws IOException {
                super(in);
            }

            private static final ConstructingObjectParser<Option, Void> PARSER = new ConstructingObjectParser<>("PhraseOptionParser",
                true, args -> {
                    Text text = new Text((String) args[0]);
                    float score = (Float) args[1];
                    String highlighted = (String) args[2];
                    Text highlightedText = highlighted == null ? null : new Text(highlighted);
                    Boolean collateMatch = (Boolean) args[3];
                    return new Option(text, highlightedText, score, collateMatch);
            });

            static {
                PARSER.declareString(constructorArg(), TEXT);
                PARSER.declareFloat(constructorArg(), SCORE);
                PARSER.declareString(optionalConstructorArg(), HIGHLIGHTED);
                PARSER.declareBoolean(optionalConstructorArg(), COLLATE_MATCH);
            }

            public static Option fromXContent(XContentParser parser) {
                    return PARSER.apply(parser, null);
            }
        }
    }
}
