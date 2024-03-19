/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.LegacyTypeFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

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

    public static boolean isMetadataField(final String field) {
        return METADATA_FIELDS.stream().map(fieldAndFormat -> fieldAndFormat.field).anyMatch(fieldName -> fieldName.equals(field));
    }

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) {
        final FetchFieldsContext fetchFieldsContext = fetchContext.fetchFieldsContext();

        final List<FieldAndFormat> fetchFields = fetchFieldsContext == null ? Collections.emptyList()
            : fetchFieldsContext.fields() == null ? Collections.emptyList()
            : fetchFieldsContext.fields();
        final FieldFetcher fieldFetcher = FieldFetcher.create(
            fetchContext.getSearchExecutionContext(),
            Stream.concat(fetchFields.stream(), METADATA_FIELDS.stream()).toList()
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
                hitContext.hit().addDocumentFields(fields.documentFields(), fields.metadataFields());
            }
        };
    }
}
