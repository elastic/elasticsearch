/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.suggest.completion;

import org.apache.lucene.search.suggest.document.TopSuggestDocsCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * Extension of the {@link TopSuggestDocsCollector} that returns top documents from the completion suggester.
 *
 * This collector groups suggestions coming from the same document but matching different contexts
 * or surface form together. When different contexts or surface forms match the same suggestion form only
 * the best one per document (sorted by weight) is kept.
 **/
class TopSuggestGroupDocsCollector extends TopSuggestDocsCollector {
    private Map<Integer, List<CharSequence>> docContexts = new HashMap<>();

    /**
     * Sole constructor
     *
     * Collects at most <code>num</code> completions
     * with corresponding document and weight
     */
    TopSuggestGroupDocsCollector(int num, boolean skipDuplicates) {
        super(num, skipDuplicates);
    }

    /**
     * Returns the contexts associated with the provided <code>doc</code>.
     */
    List<CharSequence> getContexts(int doc) {
        return docContexts.getOrDefault(doc, Collections.emptyList());
    }

    @Override
    public void collect(int docID, CharSequence key, CharSequence context, float score) throws IOException {
        int globalDoc = docID + docBase;
        boolean isNewDoc = docContexts.containsKey(globalDoc) == false;
        List<CharSequence> contexts = docContexts.computeIfAbsent(globalDoc, k -> new ArrayList<>());
        if (context != null) {
            contexts.add(context);
        }
        if (isNewDoc) {
            super.collect(docID, key, context, score);
        }
    }
}
