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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;

import java.util.Map;

/**
 * Query sub phase which returns term vectors as part of the search response.
 *
 * Specifying {@code "term_vectors": true|false|{term vectors parameters}}
 */
public class TermVectorsFetchSubPhase implements FetchSubPhase {

    @Inject
    public TermVectorsFetchSubPhase() {
    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        ImmutableMap.Builder<String, SearchParseElement> parseElements = ImmutableMap.builder();
        parseElements.put("term_vectors", new TermVectorsParseElement())
                .put("termVectors", new TermVectorsParseElement());
        return parseElements.build();
    }

    @Override
    public boolean hitsExecutionNeeded(SearchContext context) {
        return false;
    }

    @Override
    public void hitsExecute(SearchContext context, InternalSearchHit[] hits) throws ElasticsearchException {
    }

    @Override
    public boolean hitExecutionNeeded(SearchContext context) {
        return context.hasTermVectorsContext();
    }

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) throws ElasticsearchException {
        assert context.hasTermVectorsContext();
        TermVectorsResponse response = context.termVectorsContext().getResponse(context, hitContext);
        if (response.isExists()) {
            hitContext.hit().setTermVectorsResponse(response);
        }
    }
}
