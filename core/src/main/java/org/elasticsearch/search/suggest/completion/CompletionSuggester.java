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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.document.CompletionQuery;
import org.apache.lucene.search.suggest.document.TopSuggestDocs;
import org.apache.lucene.search.suggest.document.TopSuggestDocsCollector;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestContextParser;
import org.elasticsearch.search.suggest.Suggester;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CompletionSuggester extends Suggester<CompletionSuggestionContext> {

    public SuggestContextParser getContextParser() {
        return new CompletionSuggestParser(this);
    }

    @Override
    protected Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> innerExecute(String name,
            final CompletionSuggestionContext suggestionContext, final IndexSearcher searcher, CharsRefBuilder spare) throws IOException {
        final CompletionFieldMapper.CompletionFieldType fieldType = suggestionContext.getFieldType();
        if (fieldType == null) {
            throw new IllegalArgumentException("field [" + suggestionContext.getField() + "] is not a completion field");
        }
        CompletionSuggestion completionSuggestion = new CompletionSuggestion(name, suggestionContext.getSize());
        spare.copyUTF8Bytes(suggestionContext.getText());
        CompletionSuggestion.Entry completionSuggestEntry = new CompletionSuggestion.Entry(new Text(spare.toString()), 0, spare.length());
        completionSuggestion.addTerm(completionSuggestEntry);
        TopSuggestDocsCollector collector = new TopDocumentsCollector(suggestionContext.getSize());
        suggest(searcher, suggestionContext.toQuery(), collector);
        int numResult = 0;
        List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
        for (TopSuggestDocs.SuggestScoreDoc suggestScoreDoc : collector.get().scoreLookupDocs()) {
            TopDocumentsCollector.SuggestDoc suggestDoc = (TopDocumentsCollector.SuggestDoc) suggestScoreDoc;
            // collect contexts
            Map<String, Set<CharSequence>> contexts = Collections.emptyMap();
            if (fieldType.hasContextMappings() && suggestDoc.getContexts().isEmpty() == false) {
                contexts = fieldType.getContextMappings().getNamedContexts(suggestDoc.getContexts());
            }
            // collect payloads
            final Map<String, List<Object>> payload = new HashMap<>(0);
            Set<String> payloadFields = suggestionContext.getPayloadFields();
            if (payloadFields.isEmpty() == false) {
                final int readerIndex = ReaderUtil.subIndex(suggestDoc.doc, leaves);
                final LeafReaderContext subReaderContext = leaves.get(readerIndex);
                final int subDocId = suggestDoc.doc - subReaderContext.docBase;
                for (String field : payloadFields) {
                    MappedFieldType payloadFieldType = suggestionContext.getMapperService().fullName(field);
                    if (payloadFieldType != null) {
                        final AtomicFieldData data = suggestionContext.getIndexFieldDataService().getForField(payloadFieldType).load(subReaderContext);
                        final ScriptDocValues scriptValues = data.getScriptValues();
                        scriptValues.setNextDocId(subDocId);
                        payload.put(field, new ArrayList<>(scriptValues.getValues()));
                    } else {
                        throw new IllegalArgumentException("payload field [" + field + "] does not exist");
                    }
                }
            }
            if (numResult++ < suggestionContext.getSize()) {
                CompletionSuggestion.Entry.Option option = new CompletionSuggestion.Entry.Option(
                        new Text(suggestDoc.key.toString()), suggestDoc.score, contexts, payload);
                completionSuggestEntry.addOption(option);
            } else {
                break;
            }
        }
        return completionSuggestion;
    }

    private static void suggest(IndexSearcher searcher, CompletionQuery query, TopSuggestDocsCollector collector) throws IOException {
        query = (CompletionQuery) query.rewrite(searcher.getIndexReader());
        Weight weight = query.createWeight(searcher, collector.needsScores());
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

    // TODO: this should be refactored and moved to lucene
    // see https://issues.apache.org/jira/browse/LUCENE-6880
    private final static class TopDocumentsCollector extends TopSuggestDocsCollector {

        /**
         * Holds a list of suggest meta data for a doc
         */
        private final static class SuggestDoc extends TopSuggestDocs.SuggestScoreDoc {

            private List<TopSuggestDocs.SuggestScoreDoc> suggestScoreDocs;

            public SuggestDoc(int doc, CharSequence key, CharSequence context, float score) {
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

        private final static class SuggestDocPriorityQueue extends PriorityQueue<SuggestDoc> {

            public SuggestDocPriorityQueue(int maxSize) {
                super(maxSize);
            }

            @Override
            protected boolean lessThan(SuggestDoc a, SuggestDoc b) {
                if (a.score == b.score) {
                    int cmp = Lookup.CHARSEQUENCE_COMPARATOR.compare(a.key, b.key);
                    if (cmp == 0) {
                        // prefer smaller doc id, in case of a tie
                        return a.doc > b.doc;
                    } else {
                        return cmp > 0;
                    }
                }
                return a.score < b.score;
            }

            public SuggestDoc[] getResults() {
                int size = size();
                SuggestDoc[] res = new SuggestDoc[size];
                for (int i = size - 1; i >= 0; i--) {
                    res[i] = pop();
                }
                return res;
            }
        }

        private final int num;
        private final SuggestDocPriorityQueue pq;
        private final Map<Integer, SuggestDoc> scoreDocMap;

        public TopDocumentsCollector(int num) {
            super(1); // TODO hack, we don't use the underlying pq, so we allocate a size of 1
            this.num = num;
            this.scoreDocMap = new LinkedHashMap<>(num);
            this.pq = new SuggestDocPriorityQueue(num);
        }

        @Override
        public int getCountToCollect() {
            // This is only needed because we initialize
            // the base class with 1 instead of the actual num
            return num;
        }


        @Override
        protected void doSetNextReader(LeafReaderContext context) throws IOException {
            super.doSetNextReader(context);
            updateResults();
        }

        private void updateResults() {
            for (SuggestDoc suggestDoc : scoreDocMap.values()) {
                if (pq.insertWithOverflow(suggestDoc) == suggestDoc) {
                    break;
                }
            }
            scoreDocMap.clear();
        }

        @Override
        public void collect(int docID, CharSequence key, CharSequence context, float score) throws IOException {
            if (scoreDocMap.containsKey(docID)) {
                SuggestDoc suggestDoc = scoreDocMap.get(docID);
                suggestDoc.add(key, context, score);
            } else if (scoreDocMap.size() <= num) {
                scoreDocMap.put(docID, new SuggestDoc(docBase + docID, key, context, score));
            } else {
                throw new CollectionTerminatedException();
            }
        }

        @Override
        public TopSuggestDocs get() throws IOException {
            updateResults(); // to empty the last set of collected suggest docs
            TopSuggestDocs.SuggestScoreDoc[] suggestScoreDocs = pq.getResults();
            if (suggestScoreDocs.length > 0) {
                return new TopSuggestDocs(suggestScoreDocs.length, suggestScoreDocs, suggestScoreDocs[0].score);
            } else {
                return TopSuggestDocs.EMPTY;
            }
        }
    }
}
