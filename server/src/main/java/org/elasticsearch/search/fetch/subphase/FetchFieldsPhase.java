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
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A fetch sub-phase for high-level field retrieval. Given a list of fields, it
 * retrieves the field values through the relevant {@link org.elasticsearch.index.mapper.ValueFetcher}
 * and returns them as document fields.
 */
public final class FetchFieldsPhase implements FetchSubPhase {

    private static final List<FieldAndFormat> METADATA_FIELDS = List.of(
        new FieldAndFormat(IgnoredFieldMapper.NAME, null),
        new FieldAndFormat(RoutingFieldMapper.NAME, null),
        new FieldAndFormat(LegacyTypeFieldMapper.NAME, null)
    );

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) {
        FetchFieldsContext fetchFieldsContext = fetchContext.fetchFieldsContext();
        StoredFieldsContext storedFieldsContext = fetchContext.storedFieldsContext();

        // with _stored: _none_ we don't fetch metadata fields either (regardless of whether they are stored or not, for bwc reasons)
        if ((storedFieldsContext == null || storedFieldsContext.fetchFields() == false) && fetchFieldsContext == null) {
            return null;
        }

        final FieldFetcher fieldFetcher = fetchFieldsContext == null
            ? null
            : FieldFetcher.create(fetchContext.getSearchExecutionContext(), fetchFieldsContext.fields());

        final FieldFetcher fieldFetcherMetadataFields;
        if (storedFieldsContext == null || storedFieldsContext.fetchFields() == false) {
            fieldFetcherMetadataFields = null;
        } else {
            List<FieldAndFormat> fields;
            if (storedFieldsContext.fieldNames() == null) {
                fields = METADATA_FIELDS;
            } else {
                SearchExecutionContext searchExecutionContext = fetchContext.getSearchExecutionContext();
                fields = new ArrayList<>(METADATA_FIELDS);
                for (String field : storedFieldsContext.fieldNames()) {
                    // stored_fields: * gets resolved to all stored fields including metadata fields
                    // whereas fields:* will exclude metadata fields, which can only be requested explicitly
                    Collection<String> fieldNames = searchExecutionContext.getMatchingFieldNames(field);
                    for (String fieldName : fieldNames) {
                        if (fieldName.equals(SourceFieldMapper.NAME) == false && searchExecutionContext.isMetadataField(fieldName)) {
                            MappedFieldType fieldType = searchExecutionContext.getFieldType(fieldName);
                            // TODO this is for bw comp with previous behaviour of _stored_fields: there's a bunch of metadata fields that
                            // are stored and were never exposed to fetch.
                            // Some are fetchable via doc_values, but they can't be fetched either from fields, hence it makes sense to keep
                            // this conditional for the time being?
                            if (fieldType.isStored()) {
                                fields.add(new FieldAndFormat(fieldName, null));
                            }
                        }
                    }
                }
            }
            fieldFetcherMetadataFields = FieldFetcher.create(fetchContext.getSearchExecutionContext(), fields);
        }

        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                if (fieldFetcher != null) {
                    fieldFetcher.setNextReader(readerContext);
                }
                if (fieldFetcherMetadataFields != null) {
                    fieldFetcherMetadataFields.setNextReader(readerContext);
                }
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
                if (fieldFetcherMetadataFields != null) {
                    Map<String, DocumentField> metaFields = fieldFetcherMetadataFields.fetch(hitContext.source(), hitContext.docId());
                    // TODO do we need a specific method for only metadata fields and remove this one with two maps?
                    hit.addDocumentFields(Collections.emptyMap(), metaFields);
                }
            }
        };
    }
}
