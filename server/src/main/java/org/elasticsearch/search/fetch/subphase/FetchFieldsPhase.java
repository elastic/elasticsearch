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
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A fetch sub-phase for high-level field retrieval. Given a list of fields, it
 * retrieves the field values through the relevant {@link org.elasticsearch.index.mapper.ValueFetcher}
 * and returns them as document fields.
 */
public final class FetchFieldsPhase implements FetchSubPhase {

    private static final List<String> ROOT_LEVEL_METADATA_FIELD_NAMES = List.of(
        IgnoredFieldMapper.NAME,
        RoutingFieldMapper.NAME,
        LegacyTypeFieldMapper.NAME
    );

    private static final List<FieldAndFormat> ROOT_LEVEL_METADATA_FIELDS = ROOT_LEVEL_METADATA_FIELD_NAMES.stream()
        .map(field -> new FieldAndFormat(field, null))
        .toList();

    public static boolean isMetadataField(final String field) {
        return ROOT_LEVEL_METADATA_FIELD_NAMES.stream().anyMatch(f -> f.equals(field));
    }

    private static <T> List<T> emptyListIfNull(final List<T> theList) {
        return theList == null ? Collections.emptyList() : theList;
    }

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) {
        final FetchFieldsContext fetchFieldsContext = fetchContext.fetchFieldsContext();
        final StoredFieldsContext storedFieldsContext = fetchContext.storedFieldsContext();

        boolean fetchStoredFields = storedFieldsContext != null && storedFieldsContext.fetchFields();
        final List<FieldAndFormat> storedFields = storedFieldsContext == null
            ? Collections.emptyList()
            : emptyListIfNull(storedFieldsContext.fieldNames()).stream()
                .map(storedField -> new FieldAndFormat(storedField, null))
                .distinct()
                .toList();
        final List<FieldAndFormat> storedFieldsIncludingDefaultMetadataFields = fetchStoredFields
            ? Stream.concat(ROOT_LEVEL_METADATA_FIELDS.stream(), storedFields.stream()).distinct().toList()
            : Collections.emptyList();

        final List<FieldAndFormat> fetchFields = fetchFieldsContext == null
            ? Collections.emptyList()
            : emptyListIfNull(fetchFieldsContext.fields());
        final FieldFetcher fieldFetcher = FieldFetcher.create(
            fetchContext.getSearchExecutionContext(),
            Stream.concat(fetchFields.stream(), storedFieldsIncludingDefaultMetadataFields.stream()).toList()
        );

        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                fieldFetcher.setNextReader(readerContext);
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return fieldFetcher.storedFieldsSpec();
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                final FieldFetcher.DocAndMetaFields fields = fieldFetcher.fetch(hitContext.source(), hitContext.docId());
                final Map<String, DocumentField> documentFields = fields.documentFields();
                final Map<String, DocumentField> metadataFields = fields.metadataFields();
                hitContext.hit().addDocumentFields(documentFields, metadataFields);
            }
        };
    }
}
