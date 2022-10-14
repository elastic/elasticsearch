/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsContext;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class StoredFieldsPhase implements FetchSubPhase {

    @Override
    public StoredFieldsSpec storedFieldsSpec(FetchContext fetchContext) {
        if (fetchContext.storedFieldsContext() == null || fetchContext.storedFieldsContext().fetchFields() == false) {
            return StoredFieldsSpec.NO_REQUIREMENTS;
        }
        boolean requiresSource = false;
        Set<String> requiredFields = new HashSet<>();
        SearchExecutionContext sec = fetchContext.getSearchExecutionContext();
        for (String field : fetchContext.storedFieldsContext().fieldNames()) {
            if (SourceFieldMapper.NAME.equals(field)) {
                requiresSource = true;
            } else {
                Collection<String> fieldNames = sec.getMatchingFieldNames(field);
                for (String fieldName : fieldNames) {
                    MappedFieldType fieldType = sec.getFieldType(fieldName);
                    requiredFields.add(fieldType.name());
                }
            }
        }
        return new StoredFieldsSpec(requiresSource, requiredFields);
    }

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) throws IOException {
        StoredFieldsContext storedFieldsContext = fetchContext.storedFieldsContext();
        if (storedFieldsContext == null || storedFieldsContext.fetchFields() == false) {
            return null;
        }
        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) throws IOException {

            }

            @Override
            public void process(HitContext hitContext) throws IOException {

            }
        };
    }

}
