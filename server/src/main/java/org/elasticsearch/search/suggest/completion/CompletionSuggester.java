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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.suggest.document.CompletionQuery;
import org.apache.lucene.search.suggest.document.TopSuggestDocs;
import org.apache.lucene.search.suggest.document.TopSuggestDocsCollector;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.mapper.CompletionFieldMapper;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.Suggester;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CompletionSuggester extends Suggester<CompletionSuggestionContext> {

    public static final CompletionSuggester INSTANCE = new CompletionSuggester();

    private CompletionSuggester() {}

    @Override
    protected Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> innerExecute(String name,
            final CompletionSuggestionContext suggestionContext, final IndexSearcher searcher, CharsRefBuilder spare) throws IOException {
        if (suggestionContext.getFieldType() != null) {
            final CompletionFieldMapper.CompletionFieldType fieldType = suggestionContext.getFieldType();
            CompletionSuggestion completionSuggestion =
                new CompletionSuggestion(name, suggestionContext.getSize(), suggestionContext.isSkipDuplicates());
            spare.copyUTF8Bytes(suggestionContext.getText());
            CompletionSuggestion.Entry completionSuggestEntry = new CompletionSuggestion.Entry(
                new Text(spare.toString()), 0, spare.length());
            completionSuggestion.addTerm(completionSuggestEntry);
            int shardSize = suggestionContext.getShardSize() != null ? suggestionContext.getShardSize() : suggestionContext.getSize();
            TopSuggestDocsCollector collector = new TopDocumentsCollector(shardSize, suggestionContext.isSkipDuplicates());
            suggest(searcher, suggestionContext.toQuery(), collector);
            int numResult = 0;
            for (TopSuggestDocs.SuggestScoreDoc suggestScoreDoc : collector.get().scoreLookupDocs()) {
                TopDocumentsCollector.SuggestDoc suggestDoc = (TopDocumentsCollector.SuggestDoc) suggestScoreDoc;
                // collect contexts
                Map<String, Set<CharSequence>> contexts = Collections.emptyMap();
                if (fieldType.hasContextMappings() && suggestDoc.getContexts().isEmpty() == false) {
                    contexts = fieldType.getContextMappings().getNamedContexts(suggestDoc.getContexts());
                }
                if (numResult++ < suggestionContext.getSize()) {
                    CompletionSuggestion.Entry.Option option = new CompletionSuggestion.Entry.Option(suggestDoc.doc,
                        new Text(suggestDoc.key.toString()), suggestDoc.score, contexts);
                    completionSuggestEntry.addOption(option);
                } else {
                    break;
                }
            }
            return completionSuggestion;
        }
        return null;
    }

    private static void suggest(IndexSearcher searcher, CompletionQuery query, TopSuggestDocsCollector collector) throws IOException {
        query = (CompletionQuery) query.rewrite(searcher.getIndexReader());
        Weight weight = query.createWeight(searcher, collector.needsScores(), 1f);
        for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
            BulkScorer scorer = weight.bulkScorer(context);
            if (scorer != null) {
                try {
                    scorer.score(collector.getLeafCollector(context), context.reader().getLiveDocs());
                } catch (CollectionTerminatedException e) {
                    // collection was terminated prematurely
                    // continue with the following leaf
                }
            }
        }
    }

    /**
     * TODO: this should be refactored and moved to lucene see https://issues.apache.org/jira/browse/LUCENE-6880
     *
     * Custom collector that returns top documents from the completion suggester.
     * When suggestions are augmented with contexts values this collector groups suggestions coming from the same document
     * but matching different contexts together. Each document is counted as 1 entry and the provided size is the expected number
     * of documents that should be returned (not the number of suggestions).
     * This collector is also able to filter duplicate suggestion coming from different documents.
     * When different contexts match the same suggestion form only the best one (sorted by weight) is kept.
     * In order to keep this feature fast, the de-duplication of suggestions with different contexts is done
     * only on the top N*num_contexts (where N is the number of documents to return) suggestions per segment.
     * This means that skip_duplicates will visit at most N*num_contexts suggestions per segment to find unique suggestions
     * that match the input. If more than N*num_contexts suggestions are duplicated with different contexts this collector
     * will not be able to return more than one suggestion even when N is greater than 1.
     **/
    private static final class TopDocumentsCollector extends TopSuggestDocsCollector {

        /**
         * Holds a list of suggest meta data for a doc
         */
        private static final class SuggestDoc extends TopSuggestDocs.SuggestScoreDoc {

            private List<TopSuggestDocs.SuggestScoreDoc> suggestScoreDocs;

            SuggestDoc(int doc, CharSequence key, CharSequence context, float score) {
                super(doc, key, context, score);
            }

            void add(CharSequence key, CharSequence context, float score) {
                if (suggestScoreDocs == null) {
                    suggestScoreDocs = new ArrayList<>(1);
                }
                suggestScoreDocs.add(new TopSuggestDocs.SuggestScoreDoc(doc, key, context, score));
            }

            public List<CharSequence> getKeys() {
                if (suggestScoreDocs == null) {
                    return Collections.singletonList(key);
                } else {
                    List<CharSequence> keys = new ArrayList<>(suggestScoreDocs.size() + 1);
                    keys.add(key);
                    for (TopSuggestDocs.SuggestScoreDoc scoreDoc : suggestScoreDocs) {
                        keys.add(scoreDoc.key);
                    }
                    return keys;
                }
            }

            public List<CharSequence> getContexts() {
                if (suggestScoreDocs == null) {
                    if (context != null) {
                        return Collections.singletonList(context);
                    } else {
                        return Collections.emptyList();
                    }
                } else {
                    List<CharSequence> contexts = new ArrayList<>(suggestScoreDocs.size() + 1);
                    contexts.add(context);
                    for (TopSuggestDocs.SuggestScoreDoc scoreDoc : suggestScoreDocs) {
                        contexts.add(scoreDoc.context);
                    }
                    return contexts;
                }
            }
        }

        private final Map<Integer, SuggestDoc> docsMap;

        TopDocumentsCollector(int num, boolean skipDuplicates) {
            super(Math.max(1, num), skipDuplicates);
            this.docsMap = new LinkedHashMap<>(num);
        }

        @Override
        public void collect(int docID, CharSequence key, CharSequence context, float score) throws IOException {
            int globalDoc = docID + docBase;
            if (docsMap.containsKey(globalDoc)) {
                docsMap.get(globalDoc).add(key, context, score);
            } else {
                docsMap.put(globalDoc, new SuggestDoc(globalDoc, key, context, score));
                super.collect(docID, key, context, score);
            }
        }

        @Override
        public TopSuggestDocs get() throws IOException {
            TopSuggestDocs entries = super.get();
            if (entries.scoreDocs.length == 0) {
                return TopSuggestDocs.EMPTY;
            }
            // The parent class returns suggestions, not documents, and dedup only the surface form (without contexts).
            // The following code groups suggestions matching different contexts by document id and dedup the surface form + contexts
            // if needed (skip_duplicates).
            int size = entries.scoreDocs.length;
            final List<TopSuggestDocs.SuggestScoreDoc> suggestDocs = new ArrayList(size);
            final CharArraySet seenSurfaceForms = doSkipDuplicates() ? new CharArraySet(size, false) : null;
            for (TopSuggestDocs.SuggestScoreDoc suggestEntry : entries.scoreLookupDocs()) {
                final SuggestDoc suggestDoc;
                if (docsMap != null) {
                    suggestDoc = docsMap.get(suggestEntry.doc);
                } else {
                    suggestDoc = new SuggestDoc(suggestEntry.doc, suggestEntry.key, suggestEntry.context, suggestEntry.score);
                }
                if (doSkipDuplicates()) {
                    if (seenSurfaceForms.contains(suggestDoc.key)) {
                        continue;
                    }
                    seenSurfaceForms.add(suggestDoc.key);
                }
                suggestDocs.add(suggestDoc);
            }
            return new TopSuggestDocs((int) entries.totalHits,
                suggestDocs.toArray(new TopSuggestDocs.SuggestScoreDoc[0]), entries.getMaxScore());
        }
    }
}
