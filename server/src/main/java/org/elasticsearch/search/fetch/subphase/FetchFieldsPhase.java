/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
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
            : FieldFetcher.create(searchExecutionContext, fetchFieldsContext.fields());

        // NOTE: Collect stored metadata fields requested via `fields` (in FetchFieldsContext`) like for instance the _ignored source field
        final Set<FieldAndFormat> fetchContextMetadataFields = new HashSet<>();
        if (fetchFieldsContext != null && fetchFieldsContext.fields() != null && fetchFieldsContext.fields().isEmpty() == false) {
            for (final FieldAndFormat fieldAndFormat : fetchFieldsContext.fields()) {
                // NOTE: _id and _source are always retrieved anyway, no need to do it explicitly. See FieldsVisitor.
                if (SourceFieldMapper.NAME.equals(fieldAndFormat.field) || IdFieldMapper.NAME.equals(fieldAndFormat.field)) {
                    continue;
                }
                if (searchExecutionContext.isMetadataField(fieldAndFormat.field)
                    && searchExecutionContext.getFieldType(fieldAndFormat.field).isStored()) {
                    fetchContextMetadataFields.add(fieldAndFormat);
                }
            }
        }

        final FieldFetcher metadataFieldFetcher;
        if (storedFieldsContext != null
            && storedFieldsContext.fieldNames() != null
            && storedFieldsContext.fieldNames().isEmpty() == false) {
            final Set<FieldAndFormat> metadataFields = new HashSet<>(DEFAULT_METADATA_FIELDS);
            for (final String storedField : storedFieldsContext.fieldNames()) {
                final Set<String> matchingFieldNames = searchExecutionContext.getMatchingFieldNames(storedField);
                for (final String matchingFieldName : matchingFieldNames) {
                    if (SourceFieldMapper.NAME.equals(matchingFieldName) || IdFieldMapper.NAME.equals(matchingFieldName)) {
                        continue;
                    }
                    final MappedFieldType fieldType = searchExecutionContext.getFieldType(matchingFieldName);
                    // NOTE: Exclude _ignored_source when requested via wildcard '*'
                    if (matchingFieldName.equals(IgnoredSourceFieldMapper.NAME) && Regex.isSimpleMatchPattern(storedField)) {
                        continue;
                    }
                    // NOTE: checking if the field is stored is required for backward compatibility reasons and to make
                    // sure we also handle here stored fields requested via `stored_fields`, which was previously a
                    // responsibility of StoredFieldsPhase.
                    if (searchExecutionContext.isMetadataField(matchingFieldName) && fieldType.isStored()) {
                        metadataFields.add(new FieldAndFormat(matchingFieldName, null));
                    }
                }
            }
            // NOTE: Include also metadata stored fields requested via `fields`
            metadataFields.addAll(fetchContextMetadataFields);
            metadataFieldFetcher = FieldFetcher.create(searchExecutionContext, metadataFields);
        } else {
            // NOTE: Include also metadata stored fields requested via `fields`
            final Set<FieldAndFormat> allMetadataFields = new HashSet<>(DEFAULT_METADATA_FIELDS);
            allMetadataFields.addAll(fetchContextMetadataFields);
            metadataFieldFetcher = FieldFetcher.create(searchExecutionContext, allMetadataFields);
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
