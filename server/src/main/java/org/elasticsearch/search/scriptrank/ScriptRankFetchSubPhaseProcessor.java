/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.scriptrank;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceFilter;

import java.io.IOException;
import java.util.List;

public class ScriptRankFetchSubPhaseProcessor implements FetchSubPhaseProcessor {

    private final FetchContext fetchContext;
    private final List<String> fields;

    private final SourceFilter sourceFilter;

    public ScriptRankFetchSubPhaseProcessor(FetchContext fetchContext, List<String> fields) {
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
        if (fields.isEmpty() == false) {
            Source source = hitContext.source().filter(sourceFilter);
            hitContext.hit().setRankHitData(new ScriptRankHitData(source.source()));
        }
    }

    @Override
    public StoredFieldsSpec storedFieldsSpec() {
        return StoredFieldsSpec.NEEDS_SOURCE;
    }
}
