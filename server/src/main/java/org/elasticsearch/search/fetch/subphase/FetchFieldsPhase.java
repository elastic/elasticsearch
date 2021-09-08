/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Map;

/**
 * A fetch sub-phase for high-level field retrieval. Given a list of fields, it
 * retrieves the field values from _source and returns them as document fields.
 */
public final class FetchFieldsPhase implements FetchSubPhase {

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) {
        FetchFieldsContext fetchFieldsContext = fetchContext.fetchFieldsContext();
        if (fetchFieldsContext == null) {
            return null;
        }

        if (fetchContext.getSearchExecutionContext().isSourceEnabled() == false) {
            throw new IllegalArgumentException("Unable to retrieve the requested [fields] since _source is disabled " +
                "in the mappings for index [" + fetchContext.getIndexName() + "]");
        }

        FieldFetcher fieldFetcher = FieldFetcher.create(fetchContext.getSearchExecutionContext(), fetchFieldsContext.fields());

        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                fieldFetcher.setNextReader(readerContext);
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                SearchHit hit = hitContext.hit();
                SourceLookup sourceLookup = hitContext.sourceLookup();

                Map<String, DocumentField> documentFields = fieldFetcher.fetch(sourceLookup);
                for (Map.Entry<String, DocumentField> entry : documentFields.entrySet()) {
                    hit.setDocumentField(entry.getKey(), entry.getValue());
                }
            }
        };
    }
}
