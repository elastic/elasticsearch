/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.suggest.completion;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CompletionSuggester extends Suggester<CompletionSuggestionContext> {

    public static final CompletionSuggester INSTANCE = new CompletionSuggester();

    private CompletionSuggester() {}

    @Override
    protected Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> innerExecute(
        String name,
        final CompletionSuggestionContext suggestionContext,
        final IndexSearcher searcher,
        CharsRefBuilder spare
    ) throws IOException {
        if (suggestionContext.getFieldType() != null) {
            final CompletionFieldMapper.CompletionFieldType fieldType = suggestionContext.getFieldType();
            CompletionSuggestion completionSuggestion = emptySuggestion(name, suggestionContext, spare);
            int shardSize = suggestionContext.getShardSize() != null ? suggestionContext.getShardSize() : suggestionContext.getSize();
            TopSuggestGroupDocsCollector collector = new TopSuggestGroupDocsCollector(shardSize, suggestionContext.isSkipDuplicates());
            suggest(searcher, suggestionContext.toQuery(), collector);
            int numResult = 0;
            for (TopSuggestDocs.SuggestScoreDoc suggestDoc : collector.get().scoreLookupDocs()) {
                // collect contexts
                Map<String, Set<String>> contexts = Collections.emptyMap();
                if (fieldType.hasContextMappings()) {
                    List<CharSequence> rawContexts = collector.getContexts(suggestDoc.doc);
                    if (rawContexts.size() > 0) {
                        contexts = fieldType.getContextMappings().getNamedContexts(rawContexts);
                    }
                }
                if (numResult++ < suggestionContext.getSize()) {
                    CompletionSuggestion.Entry.Option option = new CompletionSuggestion.Entry.Option(
                        suggestDoc.doc,
                        new Text(suggestDoc.key.toString()),
                        suggestDoc.score,
                        contexts
                    );
                    completionSuggestion.getEntries().get(0).addOption(option);
                } else {
                    break;
                }
            }
            return completionSuggestion;
        }
        return null;
    }

    private static void suggest(IndexSearcher searcher, CompletionQuery query, TopSuggestDocsCollector collector) throws IOException {
        query = (CompletionQuery) query.rewrite(searcher);
        Weight weight = query.createWeight(searcher, collector.scoreMode(), 1f);
        for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
            BulkScorer scorer = weight.bulkScorer(context);
            if (scorer != null) {
                LeafCollector leafCollector = null;
                try {
                    leafCollector = collector.getLeafCollector(context);
                    scorer.score(leafCollector, context.reader().getLiveDocs());
                } catch (CollectionTerminatedException e) {
                    // collection was terminated prematurely
                    // continue with the following leaf
                }
                // We can only finish the leaf collector if it was actually created
                if (leafCollector != null) {
                    // We need to call finish as TopSuggestDocsCollector#finish() populates the pendingResults
                    // This is important when skipping duplicates
                    leafCollector.finish();
                }
            }
        }
    }

    @Override
    protected CompletionSuggestion emptySuggestion(String name, CompletionSuggestionContext suggestion, CharsRefBuilder spare) {
        CompletionSuggestion completionSuggestion = new CompletionSuggestion(name, suggestion.getSize(), suggestion.isSkipDuplicates());
        spare.copyUTF8Bytes(suggestion.getText());
        CompletionSuggestion.Entry completionSuggestEntry = new CompletionSuggestion.Entry(new Text(spare.toString()), 0, spare.length());
        completionSuggestion.addTerm(completionSuggestEntry);
        return completionSuggestion;
    }
}
