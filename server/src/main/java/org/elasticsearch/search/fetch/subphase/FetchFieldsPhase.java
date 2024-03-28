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
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * A fetch sub-phase for high-level field retrieval. Given a list of fields, it
 * retrieves the field values through the relevant {@link org.elasticsearch.index.mapper.ValueFetcher}
 * and returns them as document fields.
 */
public final class FetchFieldsPhase implements FetchSubPhase {

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) {
        FetchFieldsContext fetchFieldsContext = fetchContext.fetchFieldsContext();

        final FieldFetcher fieldFetcher = fetchFieldsContext == null
            ? null
            : FieldFetcher.create(fetchContext.getSearchExecutionContext(), fetchFieldsContext.fields());
        // TODO do we need to explicitly handle for the case when _ignored is explicitly requested? That's redundant?
        FieldFetcher fieldFetcherMetadataFields = FieldFetcher.create(
            fetchContext.getSearchExecutionContext(),
            Collections.singletonList(new FieldAndFormat(IgnoredFieldMapper.NAME, null))
        );

        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                if (fieldFetcher != null) {
                    fieldFetcher.setNextReader(readerContext);
                }
                fieldFetcherMetadataFields.setNextReader(readerContext);
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                if (fieldFetcher != null) {
                    return fieldFetcher.storedFieldsSpec();
                }
                return StoredFieldsSpec.NO_REQUIREMENTS;
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                SearchHit hit = hitContext.hit();
                if (fieldFetcher != null) {
                    Map<String, DocumentField> documentFields = fieldFetcher.fetch(hitContext.source(), hitContext.docId());
                    for (Map.Entry<String, DocumentField> entry : documentFields.entrySet()) {
                        hit.setDocumentField(entry.getKey(), entry.getValue());
                    }
                }
                Map<String, DocumentField> metaFields = fieldFetcherMetadataFields.fetch(hitContext.source(), hitContext.docId());
                hit.addDocumentFields(Collections.emptyMap(), metaFields);
            }
        };
    }
}
