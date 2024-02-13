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
import org.elasticsearch.index.mapper.LegacyTypeFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A fetch sub-phase for high-level field retrieval. Given a list of fields, it
 * retrieves the field values through the relevant {@link org.elasticsearch.index.mapper.ValueFetcher}
 * and returns them as document fields.
 */
public final class FetchFieldsPhase implements FetchSubPhase {

    private static final List<FieldAndFormat> METADATA_FIELDS = Arrays.asList(
        new FieldAndFormat(RoutingFieldMapper.NAME, null),
        new FieldAndFormat(IgnoredFieldMapper.NAME, null),
        new FieldAndFormat(LegacyTypeFieldMapper.NAME, null)
    );

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) {
        FetchFieldsContext fetchFieldsContext = fetchContext.fetchFieldsContext();
        if (fetchFieldsContext == null) {
            return null;
        }
        FieldFetcher fieldFetcher = FieldFetcher.create(fetchContext.getSearchExecutionContext(), fetchFieldsContext.fields());
        MetadataFetcher metadataFetcher = MetadataFetcher.create(fetchContext.getSearchExecutionContext(), METADATA_FIELDS);

        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                fieldFetcher.setNextReader(readerContext);
                metadataFetcher.setNextReader(readerContext);
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return fieldFetcher.storedFieldsSpec();
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                SearchHit hit = hitContext.hit();
                Map<String, DocumentField> documentFields = fieldFetcher.fetch(hitContext.source(), hitContext.docId());
                for (Map.Entry<String, DocumentField> entry : documentFields.entrySet()) {
                    hit.setDocumentField(entry.getKey(), entry.getValue());
                }
                hit.addDocumentFields(Collections.emptyMap(), metadataFetcher.fetch(hitContext.source(), hitContext.docId()));
            }
        };
    }
}
