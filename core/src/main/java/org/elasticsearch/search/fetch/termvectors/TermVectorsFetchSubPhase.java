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

package org.elasticsearch.search.fetch.termvectors;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.action.termvectors.TermVectorsResult;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TermVectorsFetchSubPhase implements FetchSubPhase {

    public static final ContextFactory<TermVectorsFetchContext> CONTEXT_FACTORY = new ContextFactory<TermVectorsFetchContext>() {

        @Override
        public String getName() {
            return NAMES[0];
        }

        @Override
        public TermVectorsFetchContext newContextInstance() {
            return new TermVectorsFetchContext();
        }
    };

    public TermVectorsFetchSubPhase() {
    }

    public static final String[] NAMES = {"term_vectors_fetch"};

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of("term_vectors_fetch", new TermVectorsFetchParseElement());
    }

    @Override
    public boolean hitsExecutionNeeded(SearchContext context) {
        return false;
    }

    @Override
    public void hitsExecute(SearchContext context, InternalSearchHit[] hits) {
    }

    @Override
    public boolean hitExecutionNeeded(SearchContext context) {
        return context.getFetchSubPhaseContext(CONTEXT_FACTORY).hitExecutionNeeded();
    }

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) {
        String[] fields = context.getFetchSubPhaseContext(CONTEXT_FACTORY).getFields();

        if (hitContext.hit().fieldsOrNull() == null) {
            hitContext.hit().fields(new HashMap<>());
        }
        SearchHitField hitField = hitContext.hit().fields().get(NAMES[0]);
        if (hitField == null) {
            hitField = new InternalSearchHitField(NAMES[0], new ArrayList<>(1));
            hitContext.hit().fields().put(NAMES[0], hitField);
        }

        String index = context.indexShard().indexService().index().getName();
        String type = hitContext.hit().type();
        String id = hitContext.hit().id();

        TermVectorsResult result = context.indexShard().termVectorsService().getTermVectors(
                new TermVectorsRequest(index, type, id).selectedFields(fields), index);
        try {
            getTermVectors(result, fields, false, hitField);
        } catch (IOException e) {
            throw new ElasticsearchException(e.getMessage());
        }
    }

    private void getTermVectors(TermVectorsResult result, String[] fields, boolean useResponse, SearchHitField hitField) throws IOException {
        Map<String, Integer> tv = new HashMap<>();
        Fields termVector;
        if (useResponse) {
            termVector = new TermVectorsResponse(result).getFields();
        } else {
            termVector = result.getTermVectorsByField();
        }
        try {
            for (String fieldName : fields) {
                final TermsEnum termsEnum = termVector.terms(fieldName).iterator();
                BytesRef term;
                while((term = termsEnum.next()) != null) {
                    final PostingsEnum docs = termsEnum.postings(null);
                    int freq = 0;
                    while(docs != null && docs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        freq += docs.freq();
                    }
                    tv.put(term.utf8ToString(), freq);
                }
            }
            hitField.values().add(tv);
        } catch (IOException e) {
            throw new ElasticsearchException(e.getMessage());
        }
    }
}
