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
import org.elasticsearch.index.mapper.DocValueFetcher;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Fetch sub phase which pulls data from doc values.
 *
 * Specifying {@code "docvalue_fields": ["field1", "field2"]}
 */
public final class FetchDocValuesPhase implements FetchSubPhase {
    private static final FieldAndFormat IGNORED_FIELD = new FieldAndFormat(IgnoredFieldMapper.NAME, null);

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext context) {
        FetchDocValuesContext dvContext = context.docValuesContext();
        if (dvContext == null) {
            return null;
        }
        /*
         * Its tempting to swap this to a `Map` but that'd break backwards
         * compatibility because we support fetching the same field multiple
         * times with different configuration. That isn't possible with a `Map`.
         */
        List<DocValueField> fields = new ArrayList<>();
        for (FieldAndFormat fieldAndFormat : dvContext.fields()) {
            DocValueField dvField = getDocValueField(context, fieldAndFormat);
            if (dvField == null) continue;
            fields.add(dvField);
        }

        final DocValueField ignoredField = getDocValueField(context, IGNORED_FIELD);

        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                for (DocValueField f : fields) {
                    f.fetcher.setNextReader(readerContext);
                }
                if (ignoredField != null) {
                    ignoredField.fetcher.setNextReader(readerContext);
                }
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NO_REQUIREMENTS;
            }

            @Override
            public void process(HitContext hit) throws IOException {
                for (DocValueField f : fields) {
                    List<Object> ignoredValues = new ArrayList<>();
                    List<Object> fetchValues = f.fetcher.fetchValues(hit.source(), hit.docId(), ignoredValues);
                    // Doc value fetches should not return any ignored values
                    assert ignoredValues.isEmpty() : ignoredValues;
                    DocumentField hitField = hit.hit().field(f.field);
                    if (hitField == null) {
                        hitField = new DocumentField(f.field, new ArrayList<>(fetchValues));
                        // even if we request a doc values of a meta-field (e.g. _routing),
                        // docValues fields will still be document fields, and put under "fields" section of a hit.
                        hit.hit().setDocumentField(f.field, hitField);
                    } else {
                        hitField.getValues().addAll(fetchValues);
                    }
                }
                // for BWC, we place _ignore as stored field instead
                if (ignoredField != null) {
                    List<Object> ignoredValues = new ArrayList<>();
                    List<Object> fetchValues = ignoredField.fetcher.fetchValues(hit.source(), hit.docId(), ignoredValues);
                    assert ignoredValues.isEmpty() : ignoredValues;
                    if (fetchValues.isEmpty() == false) {
                        hit.hit()
                            .addDocumentFields(
                                Map.of(),
                                Map.of(IgnoredFieldMapper.NAME, new DocumentField(IgnoredFieldMapper.NAME, fetchValues))
                            );
                    }
                }
            }
        };
    }

    private static DocValueField getDocValueField(FetchContext context, FieldAndFormat fieldAndFormat) {
        SearchExecutionContext searchExecutionContext = context.getSearchExecutionContext();
        MappedFieldType ft = searchExecutionContext.getFieldType(fieldAndFormat.field);
        if (ft == null) {
            return null;
        }
        ValueFetcher fetcher = new DocValueFetcher(
            ft.docValueFormat(fieldAndFormat.format, null),
            searchExecutionContext.getForField(ft, MappedFieldType.FielddataOperation.SEARCH)
        );
        DocValueField dvField = new DocValueField(fieldAndFormat.field, fetcher);
        return dvField;
    }

    private record DocValueField(String field, ValueFetcher fetcher) {

    }
}
