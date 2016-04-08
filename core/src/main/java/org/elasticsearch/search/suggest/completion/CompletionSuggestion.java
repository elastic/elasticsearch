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

import org.apache.lucene.search.suggest.Lookup;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.suggest.Suggest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    public static final int TYPE = 4;

    public CompletionSuggestion() {
    }

    public CompletionSuggestion(String name, int size) {
        super(name, size);
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

    @Override
    public Suggest.Suggestion<Entry> reduce(List<Suggest.Suggestion<Entry>> toReduce) {
        if (toReduce.size() == 1) {
            return toReduce.get(0);
        } else {
            // combine suggestion entries from participating shards on the coordinating node
            // the global top <code>size</code> entries are collected from the shard results
            // using a priority queue
            Comparator<Suggest.Suggestion.Entry.Option> optionComparator = sortComparator();
            OptionPriorityQueue priorityQueue = new OptionPriorityQueue(size, sortComparator());
            for (Suggest.Suggestion<Entry> entries : toReduce) {
                assert entries.getEntries().size() == 1 : "CompletionSuggestion must have only one entry";
                for (Entry.Option option : entries.getEntries().get(0)) {
                    if (option == priorityQueue.insertWithOverflow(option)) {
                        // if the current option has overflown from pq,
                        // we can assume all of the successive options
                        // from this shard result will be overflown as well
                        break;
                    }
                }
            }
            Entry options = this.entries.get(0);
            options.getOptions().clear();
            Collections.addAll(options.getOptions(), priorityQueue.get());
            return this;
        }
    }

    @Override
    public int getType() {
        return TYPE;
    }

    @Override
    protected Entry newEntry() {
        return new Entry();
    }

    public final static class Entry extends Suggest.Suggestion.Entry<CompletionSuggestion.Entry.Option> {

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
            private Map<String, Set<CharSequence>> contexts;
            private Map<String, List<Object>> payload;

            public Option(Text text, float score, Map<String, Set<CharSequence>> contexts, Map<String, List<Object>> payload) {
                super(text, score);
                this.payload = payload;
                this.contexts = contexts;
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

            public Map<String, List<Object>> getPayload() {
                return payload;
            }

            public Map<String, Set<CharSequence>> getContexts() {
                return contexts;
            }

            @Override
            public void setScore(float score) {
                super.setScore(score);
            }

            @Override
            protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
                super.innerToXContent(builder, params);
                if (payload.size() > 0) {
                    builder.startObject("payload");
                    for (Map.Entry<String, List<Object>> entry : payload.entrySet()) {
                        builder.startArray(entry.getKey());
                        for (Object payload : entry.getValue()) {
                            builder.value(payload);
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                if (contexts.size() > 0) {
                    builder.startObject("contexts");
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

            @Override
            public void readFrom(StreamInput in) throws IOException {
                super.readFrom(in);
                int payloadSize = in.readInt();
                this.payload = new LinkedHashMap<>(payloadSize);
                for (int i = 0; i < payloadSize; i++) {
                    String payloadName = in.readString();
                    int nValues = in.readVInt();
                    List<Object> values = new ArrayList<>(nValues);
                    for (int j = 0; j < nValues; j++) {
                        values.add(in.readGenericValue());
                    }
                    this.payload.put(payloadName, values);
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
                out.writeInt(payload.size());
                for (Map.Entry<String, List<Object>> entry : payload.entrySet()) {
                    out.writeString(entry.getKey());
                    List<Object> values = entry.getValue();
                    out.writeVInt(values.size());
                    for (Object value : values) {
                        out.writeGenericValue(value);
                    }
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
                stringBuilder.append(" payload:[");
                for (Map.Entry<String, List<Object>> entry : payload.entrySet()) {
                    stringBuilder.append(" ");
                    stringBuilder.append(entry.getKey());
                    stringBuilder.append(":");
                    stringBuilder.append(entry.getValue());
                }
                stringBuilder.append("]");
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
