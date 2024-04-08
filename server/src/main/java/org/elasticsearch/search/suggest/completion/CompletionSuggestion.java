/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.suggest.completion;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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

    private final boolean skipDuplicates;

    /**
     * Creates a completion suggestion given its name, size and whether it should skip duplicates
     * @param name The name for the suggestions
     * @param size The number of suggestions to return
     * @param skipDuplicates Whether duplicate suggestions should be filtered out
     */
    public CompletionSuggestion(String name, int size, boolean skipDuplicates) {
        super(name, size);
        this.skipDuplicates = skipDuplicates;
    }

    public CompletionSuggestion(StreamInput in) throws IOException {
        super(in);
        skipDuplicates = in.readBoolean();
    }

    @Override
    public String getWriteableName() {
        return CompletionSuggestionBuilder.SUGGESTION_NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(skipDuplicates);
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

    @Override
    public boolean equals(Object other) {
        return super.equals(other) && Objects.equals(skipDuplicates, ((CompletionSuggestion) other).skipDuplicates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), skipDuplicates);
    }

    private static final class OptionPriorityQueue extends PriorityQueue<ShardOptions> {
        OptionPriorityQueue(int maxSize) {
            super(maxSize);
        }

        @Override
        protected boolean lessThan(ShardOptions a, ShardOptions b) {
            int compare = COMPARATOR.compare(a.current, b.current);
            if (compare != 0) {
                return compare < 0;
            }
            ScoreDoc aDoc = a.current.getDoc();
            ScoreDoc bDoc = b.current.getDoc();
            if (aDoc.shardIndex == bDoc.shardIndex) {
                return aDoc.doc < bDoc.doc;
            }
            return aDoc.shardIndex < bDoc.shardIndex;
        }
    }

    private static class ShardOptions {
        final Iterator<Entry.Option> optionsIterator;
        Entry.Option current;

        private ShardOptions(Iterator<Entry.Option> optionsIterator) {
            assert optionsIterator.hasNext();
            this.optionsIterator = optionsIterator;
            this.current = optionsIterator.next();
            assert this.current.getDoc().shardIndex != -1 : "shardIndex is not set";
        }

        boolean advanceToNextOption() {
            if (optionsIterator.hasNext()) {
                current = optionsIterator.next();
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    public CompletionSuggestion reduce(List<Suggest.Suggestion<Entry>> toReduce) {
        if (toReduce.isEmpty()) {
            return null;
        } else {
            final CompletionSuggestion leader = (CompletionSuggestion) toReduce.get(0);
            final Entry leaderEntry = leader.getEntries().get(0);
            final String name = leader.getName();
            int size = leader.getSize();
            if (toReduce.size() == 1) {
                return leader;
            } else {
                // combine suggestion entries from participating shards on the coordinating node
                // the global top <code>size</code> entries are collected from the shard results
                // using a priority queue
                OptionPriorityQueue pq = new OptionPriorityQueue(toReduce.size());
                for (Suggest.Suggestion<Entry> suggestion : toReduce) {
                    assert suggestion.getName().equals(name) : "name should be identical across all suggestions";
                    Iterator<Entry.Option> it = ((CompletionSuggestion) suggestion).getOptions().iterator();
                    if (it.hasNext()) {
                        pq.add(new ShardOptions(it));
                    }
                }
                // Dedup duplicate suggestions (based on the surface form) if skip duplicates is activated
                final CharArraySet seenSurfaceForms = leader.skipDuplicates ? new CharArraySet(leader.getSize(), false) : null;
                final Entry entry = new Entry(leaderEntry.getText(), leaderEntry.getOffset(), leaderEntry.getLength());
                final List<Entry.Option> options = entry.getOptions();
                while (pq.size() > 0) {
                    ShardOptions top = pq.top();
                    Entry.Option current = top.current;
                    if (top.advanceToNextOption()) {
                        pq.updateTop();
                    } else {
                        // options exhausted for this shard
                        pq.pop();
                    }
                    if (leader.skipDuplicates == false || seenSurfaceForms.add(current.getText().toString())) {
                        options.add(current);
                        if (options.size() >= size) {
                            break;
                        }
                    }
                }
                final CompletionSuggestion suggestion = new CompletionSuggestion(leader.getName(), leader.getSize(), leader.skipDuplicates);
                suggestion.addTerm(entry);
                return suggestion;
            }
        }
    }

    public void setShardIndex(int shardIndex) {
        if (entries.isEmpty() == false) {
            for (Entry.Option option : getOptions()) {
                option.setShardIndex(shardIndex);
            }
        }
    }

    @Override
    protected Entry newEntry(StreamInput in) throws IOException {
        return new Entry(in);
    }

    public static final class Entry extends Suggest.Suggestion.Entry<CompletionSuggestion.Entry.Option> {

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

        public static class Option extends Suggest.Suggestion.Entry.Option {
            private final Map<String, Set<String>> contexts;
            private final ScoreDoc doc;
            private SearchHit hit;

            public static final ParseField CONTEXTS = new ParseField("contexts");

            public Option(int docID, Text text, float score, Map<String, Set<String>> contexts) {
                super(text, score);
                this.doc = new ScoreDoc(docID, score);
                this.contexts = Objects.requireNonNull(contexts, "context map cannot be null");
            }

            public Option(StreamInput in) throws IOException {
                super(in);
                this.doc = Lucene.readScoreDoc(in);
                if (in.readBoolean()) {
                    this.hit = SearchHit.readFrom(in, false);
                }
                int contextSize = in.readInt();
                this.contexts = Maps.newLinkedHashMapWithExpectedSize(contextSize);
                for (int i = 0; i < contextSize; i++) {
                    String contextName = in.readString();
                    int nContexts = in.readVInt();
                    Set<String> contexts = Sets.newHashSetWithExpectedSize(nContexts);
                    for (int j = 0; j < nContexts; j++) {
                        contexts.add(in.readString());
                    }
                    this.contexts.put(contextName, contexts);
                }
            }

            @Override
            protected void mergeInto(Suggest.Suggestion.Entry.Option otherOption) {
                // Completion suggestions are reduced by
                // org.elasticsearch.search.suggest.completion.CompletionSuggestion.reduce()
                throw new UnsupportedOperationException();
            }

            public Map<String, Set<String>> getContexts() {
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
                this.hit = hit == null ? null : hit.asUnpooled();
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.field(TEXT.getPreferredName(), getText());
                if (hit != null) {
                    hit.toInnerXContent(builder, params);
                } else {
                    builder.field(SCORE.getPreferredName(), getScore());
                }
                if (contexts.size() > 0) {
                    builder.startObject(CONTEXTS.getPreferredName());
                    for (Map.Entry<String, Set<String>> entry : contexts.entrySet()) {
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
                for (Map.Entry<String, Set<String>> entry : contexts.entrySet()) {
                    out.writeString(entry.getKey());
                    out.writeStringCollection(entry.getValue());
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
                for (Map.Entry<String, Set<String>> entry : contexts.entrySet()) {
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
