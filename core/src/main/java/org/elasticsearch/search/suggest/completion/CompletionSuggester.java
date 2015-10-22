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
import org.apache.lucene.search.suggest.xdocument.CompletionQuery;
import org.apache.lucene.search.suggest.xdocument.SuggestScoreDocPriorityQueue;
import org.apache.lucene.search.suggest.xdocument.TopSuggestDocs;
import org.apache.lucene.search.suggest.xdocument.TopSuggestDocsCollector;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestContextParser;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.completion.context.ContextMappings;

import java.io.IOException;
import java.util.*;

public class CompletionSuggester extends Suggester<CompletionSuggestionContext> {

    @Override
    public SuggestContextParser getContextParser() {
        return new CompletionSuggestParser(this);
    }

    @Override
    protected Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> innerExecute(String name,
            final CompletionSuggestionContext suggestionContext, final IndexSearcher searcher, CharsRefBuilder spare) throws IOException {
        if (suggestionContext.getFieldType() == null) {
            throw new ElasticsearchException("Field [" + suggestionContext.getField() + "] is not a completion suggest field");
        }
        CompletionSuggestion completionSuggestion = new CompletionSuggestion(name, suggestionContext.getSize());
        spare.copyUTF8Bytes(suggestionContext.getText());
        CompletionSuggestion.Entry completionSuggestEntry = new CompletionSuggestion.Entry(new StringText(spare.toString()), 0, spare.length());
        completionSuggestion.addTerm(completionSuggestEntry);
        TopSuggestDocsCollector collector = new TopDocumentsCollector(suggestionContext.getSize(), searcher.getIndexReader().numDocs());
        suggest(searcher, toQuery(suggestionContext), collector);
        int numResult = 0;
        for (TopSuggestDocs.SuggestScoreDoc suggestScoreDoc : collector.get().scoreLookupDocs()) {
            TopDocumentsCollector.SuggestDoc suggestDoc = (TopDocumentsCollector.SuggestDoc) suggestScoreDoc;
            final Map<String, Set<CharSequence>> contexts;
            if (suggestionContext.getFieldType().hasContextMappings() && suggestDoc.getContexts() != null) {
                contexts = suggestionContext.getFieldType().getContextMappings().getNamedContexts(suggestDoc.getContexts());
            } else {
                contexts = Collections.emptyMap();
            }
            final Map<String, List<Object>> payload;
            Set<String> payloadFields = suggestionContext.getPayloadFields();
            if (!payloadFields.isEmpty()) {
                int readerIndex = ReaderUtil.subIndex(suggestScoreDoc.doc, searcher.getIndexReader().leaves());
                LeafReaderContext subReaderContext = searcher.getIndexReader().leaves().get(readerIndex);
                int subDocId = suggestScoreDoc.doc - subReaderContext.docBase;
                payload = new LinkedHashMap<>(payloadFields.size());
                for (String field : payloadFields) {
                    MappedFieldType fieldType = suggestionContext.getMapperService().smartNameFieldType(field);
                    if (fieldType != null) {
                        AtomicFieldData data = suggestionContext.getFieldData().getForField(fieldType).load(subReaderContext);
                        ScriptDocValues scriptValues = data.getScriptValues();
                        scriptValues.setNextDocId(subDocId);
                        payload.put(field, new ArrayList<>(scriptValues.getValues()));
                    } else {
                        throw new ElasticsearchException("Payload field [" + field + "] does not exist");
                    }
                }
            } else {
                payload = Collections.emptyMap();
            }
            if (numResult++ < suggestionContext.getSize()) {
                completionSuggestEntry.addOption(new CompletionSuggestion.Entry.Option(new StringText(suggestScoreDoc.key.toString()), suggestScoreDoc.score, contexts, payload));
            }
        }
        return completionSuggestion;
    }

    /*
    TODO: should this be moved to CompletionSuggestionParser?
    so the CompletionSuggestionContext will have only the query instead
     */
    private CompletionQuery toQuery(CompletionSuggestionContext suggestionContext) throws IOException {
        CompletionFieldMapper.CompletionFieldType fieldType = suggestionContext.getFieldType();
        final CompletionQuery query;
        if (suggestionContext.getPrefix() != null) {
            if (suggestionContext.getFuzzyOptionsBuilder() != null) {
                CompletionSuggestionBuilder.FuzzyOptionsBuilder fuzzyOptions = suggestionContext.getFuzzyOptionsBuilder();
                query = fieldType.fuzzyQuery(suggestionContext.getPrefix().utf8ToString(),
                        Fuzziness.fromEdits(fuzzyOptions.getEditDistance()),
                        fuzzyOptions.getFuzzyPrefixLength(), fuzzyOptions.getFuzzyMinLength(),
                        fuzzyOptions.getMaxDeterminizedStates(), fuzzyOptions.isTranspositions(),
                        fuzzyOptions.isUnicodeAware());
            } else {
                query = fieldType.prefixQuery(suggestionContext.getPrefix());
            }
        } else if (suggestionContext.getRegex() != null) {
            if (suggestionContext.getFuzzyOptionsBuilder() != null) {
                throw new IllegalArgumentException("can not use 'fuzzy' options with 'regex");
            }
            CompletionSuggestionBuilder.RegexOptionsBuilder regexOptions = suggestionContext.getRegexOptionsBuilder();
            if (regexOptions == null) {
                regexOptions = new CompletionSuggestionBuilder.RegexOptionsBuilder();
            }
            query = fieldType.regexpQuery(suggestionContext.getRegex(), regexOptions.getFlagsValue(),
                    regexOptions.getMaxDeterminizedStates());
        } else {
            throw new IllegalArgumentException("'prefix' or 'regex' must be defined");
        }
        if (fieldType.hasContextMappings()) {
            ContextMappings contextMappings = fieldType.getContextMappings();
            return contextMappings.toContextQuery(query, suggestionContext.getQueryContexts());
        }
        return query;
    }

    private static class TopDocumentsCollector extends TopSuggestDocsCollector {

        private static class SuggestDoc extends TopSuggestDocs.SuggestScoreDoc {

            private List<TopSuggestDocs.SuggestScoreDoc> suggestScoreDocs;

            public SuggestDoc(int doc, CharSequence key, CharSequence context, float score) {
                super(doc, key, context, score);
            }

            public void add(CharSequence key, CharSequence context, float score) {
                if (suggestScoreDocs == null) {
                    suggestScoreDocs = new ArrayList<>();
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
                        return null;
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

            public float getScore() {
                return score;
            }

        }

        private final int maxDocs;
        private final int num;
        private final SuggestScoreDocPriorityQueue pq;
        private final Map<Integer, SuggestDoc> scoreDocMap;

        public TopDocumentsCollector(int num, int maxDocs) {
            super(num, null);
            this.maxDocs = maxDocs;
            this.num = num;
            this.scoreDocMap = new LinkedHashMap<>(num);
            this.pq = new SuggestScoreDocPriorityQueue(num);
        }

        @Override
        public int getCountToCollect() {
            return maxDocs;
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
            this.scoreDocMap.clear();
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
            updateResults();
            TopSuggestDocs.SuggestScoreDoc[] suggestScoreDocs = pq.getResults();
            if (suggestScoreDocs.length > 0) {
                return new TopSuggestDocs(suggestScoreDocs.length, suggestScoreDocs, suggestScoreDocs[0].score);
            } else {
                return TopSuggestDocs.EMPTY;
            }
        }
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
}
