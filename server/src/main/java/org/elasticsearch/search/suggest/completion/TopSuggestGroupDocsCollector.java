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
