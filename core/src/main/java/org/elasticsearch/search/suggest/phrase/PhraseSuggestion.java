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
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.Suggest.Suggestion;

import java.io.IOException;

/**
 * Suggestion entry returned from the {@link PhraseSuggester}.
 */
public class PhraseSuggestion extends Suggest.Suggestion<PhraseSuggestion.Entry> {

    public static final String NAME = "phrase";
    public static final int TYPE = 3;

    public PhraseSuggestion() {
    }

    public PhraseSuggestion(String name, int size) {
        super(name, size);
    }

    @Override
    public int getWriteableType() {
        return TYPE;
    }

    @Override
    protected String getType() {
        return NAME;
    }

    @Override
    protected Entry newEntry() {
        return new Entry();
    }

    public static PhraseSuggestion fromXContent(XContentParser parser, String name) throws IOException {
        PhraseSuggestion suggestion = new PhraseSuggestion(name, -1);
        parseEntries(parser, suggestion, PhraseSuggestion.Entry::fromXContent);
        return suggestion;
    }

    public static class Entry extends Suggestion.Entry<Suggestion.Entry.Option> {

        protected double cutoffScore = Double.MIN_VALUE;

        public Entry(Text text, int offset, int length, double cutoffScore) {
            super(text, offset, length);
            this.cutoffScore = cutoffScore;
        }

        Entry() {
        }

        /**
         * @return cutoff score for suggestions.  input term score * confidence for phrase suggest, 0 otherwise
         */
        public double getCutoffScore() {
            return cutoffScore;
        }

        @Override
        protected void merge(Suggestion.Entry<Suggestion.Entry.Option> other) {
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
        public void addOption(Suggestion.Entry.Option option) {
            if (option.getScore() > this.cutoffScore) {
                this.options.add(option);
            }
        }

        private static ObjectParser<Entry, Void> PARSER = new ObjectParser<>("PhraseSuggestionEntryParser", true, Entry::new);

        static {
            declareCommonFields(PARSER);
            PARSER.declareObjectArray(Entry::addOptions, (p,c) -> Option.fromXContent(p), new ParseField(OPTIONS));
        }

        public static Entry fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            cutoffScore = in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeDouble(cutoffScore);
        }
    }
}
