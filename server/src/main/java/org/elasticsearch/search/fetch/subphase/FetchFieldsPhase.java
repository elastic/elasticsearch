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
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.search.SearchHit;
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
        FieldFetcher fieldFetcher = null;
        MetadataFetcher metadataFetcher = null;
        if (fetchContext.getSearchExecutionContext()
            .getIndexSettings()
            .getIndexVersionCreated()
            .before(IndexVersions.DOC_VALUES_FOR_IGNORED_META_FIELD)) {
            FetchFieldsContext fetchFieldsContext = fetchContext.fetchFieldsContext();
            if (fetchFieldsContext == null) {
                return null;
            }

            fieldFetcher = FieldFetcher.create(fetchContext.getSearchExecutionContext(), fetchFieldsContext.fields());
        } else {
            final FetchFieldsContext fetchFieldsContext = fetchContext.fetchFieldsContext();
            final StoredFieldsContext storedFieldsContext = fetchContext.storedFieldsContext();
            final List<FieldAndFormat> fieldAndFormatList = fetchFieldsContext == null
                ? Collections.emptyList()
                : fetchFieldsContext.fields();
            fieldFetcher = FieldFetcher.create(fetchContext.getSearchExecutionContext(), fieldAndFormatList);
            boolean fetchStoredFields = storedFieldsContext != null && storedFieldsContext.fetchFields();
            final List<FieldAndFormat> additionalFields = getAdditionalFields(storedFieldsContext);
            metadataFetcher = MetadataFetcher.create(fetchContext.getSearchExecutionContext(), fetchStoredFields, additionalFields);
        }

        final FieldFetcher finalFieldFetcher = fieldFetcher;
        final MetadataFetcher finalMetadataFetcher = metadataFetcher;
        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                finalFieldFetcher.setNextReader(readerContext);
                if (finalMetadataFetcher != null) {
                    finalMetadataFetcher.setNextReader(readerContext);
                }
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                if (finalMetadataFetcher != null) {
                    return finalFieldFetcher.storedFieldsSpec();
                }
                return StoredFieldsSpec.NO_REQUIREMENTS;
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                SearchHit hit = hitContext.hit();
                Map<String, DocumentField> documentFields = finalFieldFetcher.fetch(hitContext.source(), hitContext.docId());
                for (Map.Entry<String, DocumentField> entry : documentFields.entrySet()) {
                    hit.setDocumentField(entry.getKey(), entry.getValue());
                }
                if (finalMetadataFetcher != null) {
                    hit.addDocumentFields(Collections.emptyMap(), finalMetadataFetcher.fetch(hitContext.source(), hitContext.docId()));
                }
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
