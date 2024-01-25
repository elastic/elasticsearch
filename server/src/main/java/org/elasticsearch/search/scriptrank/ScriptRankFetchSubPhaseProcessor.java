/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.scriptrank;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.SourceFilter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.search.ScoreMode.COMPLETE;

public class ScriptRankFetchSubPhaseProcessor implements FetchSubPhaseProcessor {

    private final FetchContext fetchContext;
    private final List<String> fields;
    private final List<Query> queries;

    private final SourceFilter sourceFilter;
    //private LeafReaderContext leafReaderContext;

    public ScriptRankFetchSubPhaseProcessor(
        FetchContext fetchContext,
        List<String> fields,
        List<QueryBuilder> queries
    ) {
        this.fetchContext = fetchContext;
        this.fields = fields;
        sourceFilter = new SourceFilter(
            fields.toArray(String[]::new),
            null
        );
        this.queries = new ArrayList<>();
        try {
            for (QueryBuilder queryBuilder : queries) {
                queryBuilder = queryBuilder.rewrite(fetchContext.getSearchExecutionContext());
                Query query = queryBuilder.toQuery(fetchContext.getSearchExecutionContext());
                this.queries.add(fetchContext.searcher().rewrite(query));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
        //this.leafReaderContext = readerContext;
    }

    @Override
    public void process(FetchSubPhase.HitContext hitContext) throws IOException {
        String index = fetchContext.getIndexName();
        if (fetchContext.getSearchExecutionContext().isSourceEnabled() == false) {
            throw new IllegalArgumentException(
                "unable to fetch fields from _source field: _source is disabled in the mappings for index [" + index + "]"
            );
        }

        Map<String, Object> filteredSource = new HashMap<>();
        if (fields.isEmpty() == false) {
            filteredSource = hitContext.source().filter(sourceFilter).source();
        }

        float[] queryScores = null;
        if (queries != null && queries.isEmpty() == false) {
            queryScores = new float[queries.size()];
            for (int i = 0; i < queries.size(); ++i) {
                var weight = queries.get(i).createWeight(fetchContext.searcher(), COMPLETE, 1f); // TODO boost is 1?
                var scorer = weight.scorer(hitContext.readerContext());
                var docId = scorer.iterator().advance(hitContext.docId());
                queryScores[i] = docId == hitContext.docId() ? scorer.score() : 0f;
            }
        }

        hitContext.hit().setRankHitData(new ScriptRankHitData(filteredSource, queryScores));
    }

    @Override
    public StoredFieldsSpec storedFieldsSpec() {
        return StoredFieldsSpec.NEEDS_SOURCE;
    }
}
