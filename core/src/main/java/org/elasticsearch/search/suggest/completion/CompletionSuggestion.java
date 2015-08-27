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
import org.apache.lucene.search.suggest.xdocument.TopSuggestDocs;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion.Entry.Option;
import org.elasticsearch.search.suggest.completion.context.ContextMappings;

import java.io.IOException;
import java.util.*;

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
public class CompletionSuggestion extends Suggest.Suggestion<CompletionSuggestion.Entry> {

    public static final int TYPE = 4;
    private ScoreDoc[] scoreDocs;

    public CompletionSuggestion() {
    }

    public CompletionSuggestion(String name, int size) {
        super(name, size);
    }

    @Override
    public int getType() {
        return TYPE;
    }

    @Override
    protected Entry newEntry() {
        return new Entry();
    }

    @Override
    public ScoreDoc[] getScoreDocs() {
        return scoreDocs;
    }

    @Override
    public List<Option> options() {
        assert entries.size() == 1;
        return entries.get(0).getOptions();
    }

    @Override
    public void innerWriteTo(StreamOutput out) throws IOException {
        super.innerWriteTo(out);
        out.writeVInt(scoreDocs.length);
        for (ScoreDoc scoreDoc : scoreDocs) {
            Lucene.writeScoreDoc(out, scoreDoc);
        }
    }

    @Override
    protected void innerReadFrom(StreamInput in) throws IOException {
        super.innerReadFrom(in);
        int size = in.readVInt();
        ScoreDoc[] scoreDocs = new ScoreDoc[size];
        for (int i = 0; i < size; i++) {
            scoreDocs[i] = Lucene.readScoreDoc(in);
        }
        this.scoreDocs = scoreDocs;
    }

    public void trim(int size) {
        if (size == 0) {
            this.scoreDocs = Lucene.EMPTY_SCORE_DOCS;
        } else if (size < scoreDocs.length) {
            ScoreDoc[] scoreDocs = new ScoreDoc[size];
            System.arraycopy(this.scoreDocs, 0, scoreDocs, 0, size);
            this.scoreDocs = scoreDocs;
        }
        this.entries.get(0).trim(size);
    }

    @Override
    public Suggest.Suggestion<Entry> reduce(List<Suggest.Suggestion<Entry>> toReduce) {
        if (toReduce.size() == 1) {
            return toReduce.get(0);
        } else if (toReduce.size() == 0) {
            return null;
        } else {
            assert this.entries.size() == 1;
            Entry entry = this.entries.get(0);
            List<Option> optionList = new ArrayList<>();
            for (Suggest.Suggestion<Entry> entries : toReduce) {
                assert entries.getEntries().size() == 1;
                optionList.addAll(entries.getEntries().get(0).getOptions());
            }
            CollectionUtil.timSort(optionList, sortComparator());
            entry.getOptions().clear();
            entry.getOptions().addAll(optionList);
        }
        return this;
    }

    public void populateEntry(String input, TopSuggestDocs topSuggestDocs, int size, ContextMappings contextMappings) {
        Map<Integer, Option> results = new LinkedHashMap<>(size);
        CompletionSuggestion.Entry completionSuggestEntry = new CompletionSuggestion.Entry(new StringText(input), 0, input.length());
        for (TopSuggestDocs.SuggestScoreDoc suggestDoc : topSuggestDocs.scoreLookupDocs()) {
            // TODO: currently we can get multiple entries with the same docID
            // this has to be fixed at the lucene level
            // This has other implications:
            // if we index a suggestion with n contexts, the suggestion and all its contexts
            // would count as n hits rather than 1, so we have to multiply the desired size
            // with n to get a suggestion with all n contexts
            final Map.Entry<String, CharSequence> contextEntry;
            if (contextMappings != null && suggestDoc.context != null) {
                contextEntry = contextMappings.getNamedContext(suggestDoc.context);
            } else {
                assert suggestDoc.context == null;
                contextEntry = null;
            }
            final Option value = results.get(suggestDoc.doc);
            if (value == null) {
                final Option option = new Option(new StringText(suggestDoc.key.toString()), suggestDoc.score, contextEntry);
                results.put(suggestDoc.doc, option);
                completionSuggestEntry.addOption(option);
            } else {
                value.addContextEntry(contextEntry);
                if (value.getScore() < suggestDoc.score) {
                    value.setScore(suggestDoc.score);
                }
            }
        }
        ScoreDoc[] scoreDocs = new ScoreDoc[results.size()];
        Iterator<Map.Entry<Integer, Option>> iterator = results.entrySet().iterator();
        for (int i = 0; iterator.hasNext(); i++) {
            Map.Entry<Integer, Option> next = iterator.next();
            scoreDocs[i] = new ScoreDoc(next.getKey(), next.getValue().getScore());
        }
        this.scoreDocs = scoreDocs;
        if (this.entries.size() > 0) {
            throw new IllegalStateException("only a single entry is allowed for CompletionSuggestion");
        }
        this.entries.add(completionSuggestEntry);
    }

    public static class Entry extends Suggest.Suggestion.Entry<CompletionSuggestion.Entry.Option> {

        private Entry(Text text, int offset, int length) {
            super(text, offset, length);
        }

        protected Entry() {
            super();
        }

        @Override
        public void addOption(Option option) {
            super.addOption(option);
        }

        @Override
        protected Option newOption() {
            return new Option();
        }

        public static class Option extends Suggest.Suggestion.Entry.Option {
            private Map<String, Set<CharSequence>> contexts = new TreeMap<>();
            private SearchHit hit;

            private Option(Text text, float score, Map.Entry<String, CharSequence> contextEntry) {
                super(text, score);
                addContextEntry(contextEntry);
            }

            protected Option() {
                super();
            }

            public void hit(InternalSearchHit hit) {
                this.hit = hit;
            }

            public SearchHit hit() {
                return hit;
            }

            @Override
            protected void setScore(float score) {
                super.setScore(score);
            }

            void addContextEntry(Map.Entry<String, CharSequence> entry) {
                if (entry != null) {
                    Set<CharSequence> namedContext = contexts.get(entry.getKey());
                    if (namedContext == null) {
                        namedContext = new HashSet<>();
                    }
                    CharSequence value = entry.getValue();
                    if (value != null) {
                        namedContext.add(value);
                    }
                    if (namedContext.size() > 0) {
                        contexts.put(entry.getKey(), namedContext);
                    }
                }
            }

            public Map<String, Set<CharSequence>> getContexts() {
                return contexts;
            }

            @Override
            protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
                builder.field("text", getText());
                if (hit == null) {
                    builder.field("score", getScore());
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
                if (hit != null) {
                    ((InternalSearchHit) hit).innerToXContent(builder, params);
                }
                return builder;
            }

            @Override
            public void readFrom(StreamInput in) throws IOException {
                super.readFrom(in);
                int size = in.readInt();
                for (int i = 0; i < size; i++) {
                    String contextName = in.readString();
                    int nContexts = in.readVInt();
                    Set<CharSequence> contexts = new HashSet<>(nContexts);
                    for (int j = 0; j < nContexts; j++) {
                        contexts.add(in.readString());
                    }
                    this.contexts.put(contextName, contexts);
                }
                if (in.readBoolean()) {
                    hit = InternalSearchHit.readSearchHit(in, InternalSearchHits.streamContext().streamShardTarget(InternalSearchHits.StreamContext.ShardTargetType.STREAM));
                }
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeInt(contexts.size());
                for (Map.Entry<String, Set<CharSequence>> entry : contexts.entrySet()) {
                    out.writeString(entry.getKey());
                    out.writeVInt(entry.getValue().size());
                    for (CharSequence ctx : entry.getValue()) {
                        out.writeString(ctx.toString());
                    }
                }
                if (hit != null) {
                    out.writeBoolean(true);
                    hit.writeTo(out);
                } else {
                    out.writeBoolean(false);
                }
            }
        }
    }

}
