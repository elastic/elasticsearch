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
package org.elasticsearch.search.suggest.completion;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.suggest.Lookup;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.Suggest.Suggestion;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.search.suggest.Suggest.COMPARATOR;

/**
 * Suggestion response for {@link CompletionSuggester} results
 *
 * Response format for each entry:
 * {
 *     "text" : STRING
 *     "score" : FLOAT
 *     "contexts" : CONTEXTS
 * }
 *
 * CONTEXTS : {
 *     "CONTEXT_NAME" : ARRAY,
 *     ..
 * }
 *
 */
public final class CompletionSuggestion extends Suggest.Suggestion<CompletionSuggestion.Entry> {

    public static final String NAME = "completion";

    public static final int TYPE = 4;

    private boolean skipDuplicates;

    public CompletionSuggestion() {
    }

    /**
     * Ctr
     * @param name The name for the suggestions
     * @param size The number of suggestions to return
     * @param skipDuplicates Whether duplicate suggestions should be filtered out
     */
    public CompletionSuggestion(String name, int size, boolean skipDuplicates) {
        super(name, size);
        this.skipDuplicates = skipDuplicates;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.getVersion().onOrAfter(Version.V_6_1_0)) {
            skipDuplicates = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_6_1_0)) {
            out.writeBoolean(skipDuplicates);
        }
    }

    /**
     * @return the result options for the suggestion
     */
    public List<Entry.Option> getOptions() {
        if (entries.isEmpty() == false) {
            assert entries.size() == 1 : "CompletionSuggestion must have only one entry";
            return entries.get(0).getOptions();
        } else {
            return Collections.emptyList();
        }
    }

    /**
     * @return whether there is any hits for the suggestion
     */
    public boolean hasScoreDocs() {
        return getOptions().size() > 0;
    }

    public static CompletionSuggestion fromXContent(XContentParser parser, String name) throws IOException {
        CompletionSuggestion suggestion = new CompletionSuggestion(name, -1, false);
        parseEntries(parser, suggestion, CompletionSuggestion.Entry::fromXContent);
        return suggestion;
    }

    private static final class OptionPriorityQueue extends org.apache.lucene.util.PriorityQueue<Entry.Option> {

        private final Comparator<Suggest.Suggestion.Entry.Option> comparator;

        OptionPriorityQueue(int maxSize, Comparator<Suggest.Suggestion.Entry.Option> comparator) {
            super(maxSize);
            this.comparator = comparator;
        }

        @Override
        protected boolean lessThan(Entry.Option a, Entry.Option b) {
            int cmp = comparator.compare(a, b);
            if (cmp != 0) {
                return cmp > 0;
            }
            return Lookup.CHARSEQUENCE_COMPARATOR.compare(a.getText().string(), b.getText().string()) > 0;
        }

        Entry.Option[] get() {
            int size = size();
            Entry.Option[] results = new Entry.Option[size];
            for (int i = size - 1; i >= 0; i--) {
                results[i] = pop();
            }
            return results;
        }
    }

    /**
     * Reduces suggestions to a single suggestion containing at most
     * top {@link CompletionSuggestion#getSize()} options across <code>toReduce</code>
     */
    public static CompletionSuggestion reduceTo(List<Suggest.Suggestion<Entry>> toReduce) {
        if (toReduce.isEmpty()) {
            return null;
        } else {
            final CompletionSuggestion leader = (CompletionSuggestion) toReduce.get(0);
            final Entry leaderEntry = leader.getEntries().get(0);
            final String name = leader.getName();
            if (toReduce.size() == 1) {
                return leader;
            } else {
                // combine suggestion entries from participating shards on the coordinating node
                // the global top <code>size</code> entries are collected from the shard results
                // using a priority queue
                OptionPriorityQueue priorityQueue = new OptionPriorityQueue(leader.getSize(), COMPARATOR);
                // Dedup duplicate suggestions (based on the surface form) if skip duplicates is activated
                final CharArraySet seenSurfaceForms = leader.skipDuplicates ? new CharArraySet(leader.getSize(), false) : null;
                for (Suggest.Suggestion<Entry> suggestion : toReduce) {
                    assert suggestion.getName().equals(name) : "name should be identical across all suggestions";
                    for (Entry.Option option : ((CompletionSuggestion) suggestion).getOptions()) {
                        if (leader.skipDuplicates) {
                            assert ((CompletionSuggestion) suggestion).skipDuplicates;
                            String text = option.getText().string();
                            if (seenSurfaceForms.contains(text)) {
                                continue;
                            }
                            seenSurfaceForms.add(text);
                        }
                        if (option == priorityQueue.insertWithOverflow(option)) {
                            // if the current option has overflown from pq,
                            // we can assume all of the successive options
                            // from this shard result will be overflown as well
                            break;
                        }
                    }
                }
                final CompletionSuggestion suggestion = new CompletionSuggestion(leader.getName(), leader.getSize(), leader.skipDuplicates);
                final Entry entry = new Entry(leaderEntry.getText(), leaderEntry.getOffset(), leaderEntry.getLength());
                Collections.addAll(entry.getOptions(), priorityQueue.get());
                suggestion.addTerm(entry);
                return suggestion;
            }
        }
    }

    @Override
    public Suggest.Suggestion<Entry> reduce(List<Suggest.Suggestion<Entry>> toReduce) {
        return reduceTo(toReduce);
    }

    public void setShardIndex(int shardIndex) {
        if (entries.isEmpty() == false) {
            for (Entry.Option option : getOptions()) {
                option.setShardIndex(shardIndex);
            }
        }
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

    public static final class Entry extends Suggest.Suggestion.Entry<CompletionSuggestion.Entry.Option> {

        public Entry(Text text, int offset, int length) {
            super(text, offset, length);
        }

        Entry() {
        }

        @Override
        protected Option newOption() {
            return new Option();
        }

        private static ObjectParser<Entry, Void> PARSER = new ObjectParser<>("CompletionSuggestionEntryParser", true,
                Entry::new);

        static {
            declareCommonFields(PARSER);
            PARSER.declareObjectArray(Entry::addOptions, (p,c) -> Option.fromXContent(p), new ParseField(OPTIONS));
        }

        public static Entry fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        public static class Option extends Suggest.Suggestion.Entry.Option {
            private Map<String, Set<CharSequence>> contexts = Collections.emptyMap();
            private ScoreDoc doc;
            private SearchHit hit;

            public static final ParseField CONTEXTS = new ParseField("contexts");

            public Option(int docID, Text text, float score, Map<String, Set<CharSequence>> contexts) {
                super(text, score);
                this.doc = new ScoreDoc(docID, score);
                this.contexts = Objects.requireNonNull(contexts, "context map cannot be null");
            }

            protected Option() {
                super();
            }

            @Override
            protected void mergeInto(Suggest.Suggestion.Entry.Option otherOption) {
                // Completion suggestions are reduced by
                // org.elasticsearch.search.suggest.completion.CompletionSuggestion.reduce()
                throw new UnsupportedOperationException();
            }

            public Map<String, Set<CharSequence>> getContexts() {
                return contexts;
            }

            public ScoreDoc getDoc() {
                return doc;
            }

            public SearchHit getHit() {
                return hit;
            }

            public void setShardIndex(int shardIndex) {
                this.doc.shardIndex = shardIndex;
            }

            public void setHit(SearchHit hit) {
                this.hit = hit;
            }

            @Override
            protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
                builder.field(TEXT.getPreferredName(), getText());
                if (hit != null) {
                    hit.toInnerXContent(builder, params);
                } else {
                    builder.field(SCORE.getPreferredName(), getScore());
                }
                if (contexts.size() > 0) {
                    builder.startObject(CONTEXTS.getPreferredName());
                    for (Map.Entry<String, Set<CharSequence>> entry : contexts.entrySet()) {
                        builder.startArray(entry.getKey());
                        for (CharSequence context : entry.getValue()) {
                            builder.value(context.toString());
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                return builder;
            }

            private static ObjectParser<Map<String, Object>, Void> PARSER = new ObjectParser<>("CompletionOptionParser",
                    true, HashMap::new);

            static {
                SearchHit.declareInnerHitsParseFields(PARSER);
                PARSER.declareString((map, value) -> map.put(Suggestion.Entry.Option.TEXT.getPreferredName(), value),
                        Suggestion.Entry.Option.TEXT);
                PARSER.declareFloat((map, value) -> map.put(Suggestion.Entry.Option.SCORE.getPreferredName(), value),
                        Suggestion.Entry.Option.SCORE);
                PARSER.declareObject((map, value) -> map.put(CompletionSuggestion.Entry.Option.CONTEXTS.getPreferredName(), value),
                        (p,c) -> parseContexts(p), CompletionSuggestion.Entry.Option.CONTEXTS);
            }

            private static Map<String, Set<CharSequence>> parseContexts(XContentParser parser) throws IOException {
                Map<String, Set<CharSequence>> contexts = new HashMap<>();
                while((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
                    String key = parser.currentName();
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser::getTokenLocation);
                    Set<CharSequence> values = new HashSet<>();
                    while((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.currentToken(), parser::getTokenLocation);
                        values.add(parser.text());
                    }
                    contexts.put(key, values);
                }
                return contexts;
            }

            public static Option fromXContent(XContentParser parser) {
                Map<String, Object> values = PARSER.apply(parser, null);

                Text text = new Text((String) values.get(Suggestion.Entry.Option.TEXT.getPreferredName()));
                Float score = (Float) values.get(Suggestion.Entry.Option.SCORE.getPreferredName());
                @SuppressWarnings("unchecked")
                Map<String, Set<CharSequence>> contexts = (Map<String, Set<CharSequence>>) values
                        .get(CompletionSuggestion.Entry.Option.CONTEXTS.getPreferredName());
                if (contexts == null) {
                    contexts = Collections.emptyMap();
                }

                SearchHit hit = null;
                // the option either prints SCORE or inlines the search hit
                if (score == null) {
                    hit = SearchHit.createFromMap(values);
                    score = hit.getScore();
                }
                CompletionSuggestion.Entry.Option option = new CompletionSuggestion.Entry.Option(-1, text, score, contexts);
                option.setHit(hit);
                return option;
            }

            @Override
            public void readFrom(StreamInput in) throws IOException {
                super.readFrom(in);
                this.doc = Lucene.readScoreDoc(in);
                if (in.readBoolean()) {
                    this.hit = SearchHit.readSearchHit(in);
                }
                int contextSize = in.readInt();
                this.contexts = new LinkedHashMap<>(contextSize);
                for (int i = 0; i < contextSize; i++) {
                    String contextName = in.readString();
                    int nContexts = in.readVInt();
                    Set<CharSequence> contexts = new HashSet<>(nContexts);
                    for (int j = 0; j < nContexts; j++) {
                        contexts.add(in.readString());
                    }
                    this.contexts.put(contextName, contexts);
                }
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                Lucene.writeScoreDoc(out, doc);
                if (hit != null) {
                    out.writeBoolean(true);
                    hit.writeTo(out);
                } else {
                    out.writeBoolean(false);
                }
                out.writeInt(contexts.size());
                for (Map.Entry<String, Set<CharSequence>> entry : contexts.entrySet()) {
                    out.writeString(entry.getKey());
                    out.writeVInt(entry.getValue().size());
                    for (CharSequence ctx : entry.getValue()) {
                        out.writeString(ctx.toString());
                    }
                }
            }

            @Override
            public String toString() {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("text:");
                stringBuilder.append(getText());
                stringBuilder.append(" score:");
                stringBuilder.append(getScore());
                stringBuilder.append(" context:[");
                for (Map.Entry<String, Set<CharSequence>> entry: contexts.entrySet()) {
                    stringBuilder.append(" ");
                    stringBuilder.append(entry.getKey());
                    stringBuilder.append(":");
                    stringBuilder.append(entry.getValue());
                }
                stringBuilder.append("]");
                return stringBuilder.toString();
            }
        }
    }

}
