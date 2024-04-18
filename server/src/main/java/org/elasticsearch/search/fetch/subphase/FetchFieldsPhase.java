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
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.LegacyTypeFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;

/**
 * A fetch sub-phase for high-level field retrieval. Given a list of fields, it
 * retrieves the field values through the relevant {@link org.elasticsearch.index.mapper.ValueFetcher}
 * and returns them as document fields.
 */
public final class FetchFieldsPhase implements FetchSubPhase {

    private static final List<FieldAndFormat> DEFAULT_METADATA_FIELDS = List.of(
        new FieldAndFormat(IgnoredFieldMapper.NAME, null),
        new FieldAndFormat(RoutingFieldMapper.NAME, null),
        // will only be fetched when mapped (older archived indices)
        new FieldAndFormat(LegacyTypeFieldMapper.NAME, null)
    );

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) {
        final FetchFieldsContext fetchFieldsContext = fetchContext.fetchFieldsContext();
        final StoredFieldsContext storedFieldsContext = fetchContext.storedFieldsContext();

        boolean fetchStoredFields = storedFieldsContext != null && storedFieldsContext.fetchFields();
        if (fetchFieldsContext == null && fetchStoredFields == false) {
            return null;
        }

        final SearchExecutionContext searchExecutionContext = fetchContext.getSearchExecutionContext();
        final FieldFetcher fieldFetcher = fetchFieldsContext == null ? null
            : fetchFieldsContext.fields() == null ? null
            : fetchFieldsContext.fields().isEmpty() ? null
            // we want to skip metadata fields if we have a wildcard pattern
            : FieldFetcher.create(
                searchExecutionContext,
                fetchFieldsContext.fields(),
                (field, ft) -> true,
                (field, ft) -> searchExecutionContext.isMetadataField(field) == false
            );

        final FieldFetcher metadataFieldFetcher;
        if (storedFieldsContext != null
            && storedFieldsContext.fieldNames() != null
            && storedFieldsContext.fieldNames().isEmpty() == false) {
            final Set<FieldAndFormat> metadataFields = new HashSet<>(DEFAULT_METADATA_FIELDS);
            for (final String storedField : storedFieldsContext.fieldNames()) {
                metadataFields.add(new FieldAndFormat(storedField, null));
            }
            BiPredicate<String, MappedFieldType> includePredicate = (field, ft) -> field.equals(SourceFieldMapper.NAME) == false
                && field.equals(IdFieldMapper.NAME) == false
                && searchExecutionContext.isMetadataField(field)
                && ft.isStored();
            metadataFieldFetcher = FieldFetcher.create(searchExecutionContext, metadataFields, includePredicate, includePredicate);
        } else {
            metadataFieldFetcher = FieldFetcher.create(
                searchExecutionContext,
                DEFAULT_METADATA_FIELDS,
                (ft, s) -> true,
                (field, ft) -> true
            );
        }
        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                if (fieldFetcher != null) {
                    fieldFetcher.setNextReader(readerContext);
                }
                metadataFieldFetcher.setNextReader(readerContext);
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                if (fieldFetcher != null) {
                    return metadataFieldFetcher.storedFieldsSpec().merge(fieldFetcher.storedFieldsSpec());
                }
                return metadataFieldFetcher.storedFieldsSpec();
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                final Map<String, DocumentField> fields = fieldFetcher != null
                    ? fieldFetcher.fetch(hitContext.source(), hitContext.docId())
                    : Collections.emptyMap();
                final Map<String, DocumentField> metadataFields = metadataFieldFetcher.fetch(hitContext.source(), hitContext.docId());
                hitContext.hit().addDocumentFields(fields, metadataFields);
            }
        };
    }
}
