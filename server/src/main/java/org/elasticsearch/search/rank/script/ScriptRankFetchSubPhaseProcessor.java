/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.script;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.SourceFilter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScriptRankFetchSubPhaseProcessor implements FetchSubPhaseProcessor {

    private final FetchContext fetchContext;
    private final List<String> fields;

    private final SourceFilter sourceFilter;

    private final Logger logger = LogManager.getLogger(ScriptRankFetchSubPhaseProcessor.class);

    public ScriptRankFetchSubPhaseProcessor(
        FetchContext fetchContext,
        List<String> fields
    ) {
        this.fetchContext = fetchContext;
        this.fields = fields;
        sourceFilter = new SourceFilter(
            fields.toArray(String[]::new),
            null
        );
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {

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


        hitContext.hit().setRankHitData(new ScriptRankHitData(filteredSource));
    }

    @Override
    public StoredFieldsSpec storedFieldsSpec() {
        return StoredFieldsSpec.NEEDS_SOURCE;
    }
}
