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
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A fetch sub-phase for high-level field retrieval. Given a list of fields, it
 * retrieves the field values and returns them as document fields.
 */
public final class FetchFieldsPhase implements FetchSubPhase {

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) {
        final FetchFieldsContext fetchFieldsContext = fetchContext.fetchFieldsContext();
        final StoredFieldsContext storedFieldsContext = fetchContext.storedFieldsContext();
        final List<FieldAndFormat> fetchFields = fetchFieldsContext == null ? Collections.emptyList() : fetchFieldsContext.fields();
        final FieldFetcher fieldFetcher = FieldFetcher.create(fetchContext.getSearchExecutionContext(), fetchFields);
        final List<FieldAndFormat> additionalFields = getAdditionalFields(storedFieldsContext);
        boolean fetchStoredFields = storedFieldsContext != null && storedFieldsContext.fetchFields();
        final MetadataFetcher metadataFetcher = MetadataFetcher.create(
            fetchContext.getSearchExecutionContext(),
            fetchStoredFields,
            additionalFields
        );

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
                final Map<String, DocumentField> documentFields = fieldFetcher.fetch(hitContext.source(), hitContext.docId());
                final Map<String, DocumentField> metadataFields = metadataFetcher.fetch(hitContext.source(), hitContext.docId());
                hitContext.hit().addDocumentFields(documentFields, metadataFields);
            }
        };
    }

    private static List<FieldAndFormat> getAdditionalFields(final StoredFieldsContext storedFieldsContext) {
        List<String> fieldNames = new ArrayList<>(1);
        if (storedFieldsContext != null && storedFieldsContext.fieldNames() != null) {
            fieldNames = storedFieldsContext.fieldNames();
        }
        final List<FieldAndFormat> additionalFields = new ArrayList<>(1);
        for (String fieldName : fieldNames) {
            if (fieldName.matches("\\*")) {
                additionalFields.add(new FieldAndFormat("_size", null));
            }
        }
        return additionalFields;
    }
}
