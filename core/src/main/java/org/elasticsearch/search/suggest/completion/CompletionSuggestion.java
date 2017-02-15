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

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.suggest.Lookup;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.suggest.Suggest;

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

import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownField;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownToken;
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

    private static final String NAME = "completion";

    public static final int TYPE = 4;

    public CompletionSuggestion() {
    }

    public CompletionSuggestion(String name, int size) {
        super(name, size);
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
                for (Suggest.Suggestion<Entry> suggestion : toReduce) {
                    assert suggestion.getName().equals(name) : "name should be identical across all suggestions";
                    for (Entry.Option option : ((CompletionSuggestion) suggestion).getOptions()) {
                        if (option == priorityQueue.insertWithOverflow(option)) {
                            // if the current option has overflown from pq,
                            // we can assume all of the successive options
                            // from this shard result will be overflown as well
                            break;
                        }
                    }
                }
                final CompletionSuggestion suggestion = new CompletionSuggestion(leader.getName(), leader.getSize());
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

        protected Entry() {
            super();
        }

        @Override
        protected Option newOption() {
            return new Option();
        }

        public static class Option extends Suggest.Suggestion.Entry.Option {
            private Map<String, Set<CharSequence>> contexts = Collections.emptyMap();
            private ScoreDoc doc;
            private SearchHit hit;

            public static final ParseField CONTEXT = new ParseField("contexts");

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
                    builder.startObject(CONTEXT.getPreferredName());
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

            public static Option fromXContent(XContentParser parser) throws IOException  {
                XContentParser.Token token = parser.nextToken();
                String currentFieldName = null;
                String text = null;
                float score = -1;
                Map<String, Set<CharSequence>> contexts = new HashMap<>();
                SearchHit searchHit = null;
                boolean foundHit = false;
                try (XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType()).startObject()) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                            if ("text".equals(currentFieldName) == false && "score".equals(currentFieldName) == false
                                    && "contexts".equals(currentFieldName) == false) {
                                builder.copyCurrentStructure(parser);
                                foundHit = true;
                            }
                        } else if (token.isValue()) {
                            if ("text".equals(currentFieldName)) {
                                text = parser.text();
                            } else if ("score".equals(currentFieldName)) {
                                score = parser.floatValue();
                            } else {
                                throwUnknownField(currentFieldName, parser.getTokenLocation());
                            }
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            if ("contexts".equals(currentFieldName)) {
                                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                                    if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                                        currentFieldName = parser.currentName();
                                    } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                                        Set<CharSequence> set = new HashSet<>();
                                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                            set.add(parser.text());
                                        }
                                        contexts.put(currentFieldName, set);
                                    } else {
                                        throw new IllegalStateException();
                                    }
                                }
                            } else {
                                throwUnknownField(currentFieldName, parser.getTokenLocation());
                            }
                        } else if (token == XContentParser.Token.START_ARRAY) {
                            throwUnknownToken(token, parser.getTokenLocation());
                        }
                    }
                    BytesReference bytes = builder.endObject().bytes();
                    if (foundHit) {
                        try (XContentParser hitsParser = parser.contentType().xContent().createParser(parser.getXContentRegistry(),
                                bytes)) {
                            hitsParser.nextToken();
                            searchHit = SearchHit.fromXContent(hitsParser);
                            score = searchHit.getScore();
                        }
                    }
                }
                Option option = new Option(1, new Text(text), score, contexts);
                option.setHit(searchHit);
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
