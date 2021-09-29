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
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * A fetch sub-phase for high-level field retrieval. Given a list of fields, it
 * retrieves the field values from _source and returns them as document fields.
 */
public final class FetchFieldsPhase implements FetchSubPhase {
    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) {
        FetchFieldsContext fetchFieldsContext = fetchContext.fetchFieldsContext();
        if (fetchFieldsContext == null) {
            return null;
        }

        if (fetchContext.getSearchExecutionContext().isSourceEnabled() == false) {
            throw new IllegalArgumentException("Unable to retrieve the requested [fields] since _source is disabled " +
                "in the mappings for index [" + fetchContext.getIndexName() + "]");
        }

        FieldFetcher fieldFetcher = FieldFetcher.create(fetchContext.getSearchExecutionContext(), fetchFieldsContext.fields());
//        final FetchDocValuesContext fdv = fetchContext.docValuesContext();

        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                fieldFetcher.setNextReader(readerContext);
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                SearchHit hit = hitContext.hit();
                SourceLookup sourceLookup = hitContext.sourceLookup();
                
                DocumentField ignored = hit.field(IgnoredFieldMapper.NAME);
                if (ignored != null) {
                    for (Object ignoredField : ignored.getValues()) {
                        String ignoredFieldName = ignoredField.toString();
                        boolean wasRequested = fetchFieldsContext.fields()
                            .stream()
                            .anyMatch(f -> Regex.simpleMatch(f.field, ignoredFieldName));
                        if (wasRequested == false) {
                            // We don't care that this field was ignored at index time - the user didn't request to see it.
                            continue;
                        }
                        SearchExecutionContext sec = fetchContext.getSearchExecutionContext();
                        MappedFieldType fieldType = sec.getFieldType(ignoredFieldName);
                        ValueFetcher valueFetcher = fieldType.valueFetcher(sec, null);
                        List<Object> docValues = valueFetcher.fetchValues(sourceLookup);
                        List<Object> sourceValues = sourceLookup.extractRawValues(ignoredFieldName);

                        int lastDot = ignoredFieldName.lastIndexOf(".");
                        if (sourceValues.isEmpty() && lastDot > 0) {
                            // Looks like a multifield e.g. foo.keyword
                            // Will need to strip "keyword" part from fieldname to retrieve foo from source.
                            // Ideally MappedFieldType would have a "isMultiField()" method but need to do this "suck it and see" approach
                            // to retrieving from source
                            String parentFieldName = ignoredFieldName.substring(0, lastDot);
                            sourceValues = sourceLookup.extractRawValues(parentFieldName);
                        }
                        // De-duplicate source values and remove any non-ignored values that were seen in doc values
                        // Not perfect because a valid normalized keyword field can differ from source value meaning we
                        // will return the source value as example of ignored field when it wasn't ignored.
                        HashSet<Object> ignoredSourceValues = new HashSet<Object>(sourceValues);
                        ignoredSourceValues.removeAll(docValues);
                        if (ignoredSourceValues.isEmpty() == false) {
                            hit.setIgnoredFieldValues(ignoredFieldName, new DocumentField(ignoredFieldName, List.of(ignoredSourceValues)));
                        }
                    }
                }

                Map<String, DocumentField> documentFields = fieldFetcher.fetch(sourceLookup);
                for (Map.Entry<String, DocumentField> entry : documentFields.entrySet()) {
                    hit.setDocumentField(entry.getKey(), entry.getValue());
                }
            }
        };
    }
}
