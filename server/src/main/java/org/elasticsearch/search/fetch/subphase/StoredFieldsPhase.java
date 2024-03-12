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
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Process stored fields loaded from a HitContext into DocumentFields
 */
public class StoredFieldsPhase implements FetchSubPhase {

    /** Associates a field name with a mapped field type and whether or not it is a metadata field */
    private record StoredField(String name, MappedFieldType ft, boolean isMetadataField) {

        /** Processes a set of stored fields using field type information */
        List<Object> process(Map<String, List<Object>> loadedFields) {
            List<Object> inputs = loadedFields.get(ft.name());
            if (inputs == null) {
                return List.of();
            }
            return inputs.stream().map(ft::valueForDisplay).toList();
        }

        boolean hasValue(Map<String, List<Object>> loadedFields) {
            return loadedFields.containsKey(ft.name());
        }

    }

    private static final List<StoredField> METADATA_FIELDS = List.of(
        new StoredField("_routing", RoutingFieldMapper.FIELD_TYPE, true),
        new StoredField("_ignored", IgnoredFieldMapper.FIELD_TYPE, true),
        // pre-6.0 indexes can return a _type field, this will be valueless in modern indexes and ignored
        new StoredField("_type", LegacyTypeFieldMapper.FIELD_TYPE, true)
    );

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) {
        StoredFieldsContext storedFieldsContext = fetchContext.storedFieldsContext();
        if (storedFieldsContext == null || storedFieldsContext.fetchFields() == false) {
            return null;
        }

        // build the StoredFieldsSpec and a list of StoredField records to process
        List<StoredField> storedFields = new ArrayList<>(METADATA_FIELDS);
        Set<String> fieldsToLoad = new HashSet<>();
        if (storedFieldsContext.fieldNames() != null) {
            SearchExecutionContext sec = fetchContext.getSearchExecutionContext();
            for (String field : storedFieldsContext.fieldNames()) {
                if (SourceFieldMapper.NAME.equals(field) == false) {
                    Collection<String> fieldNames = sec.getMatchingFieldNames(field);
                    for (String fieldName : fieldNames) {
                        MappedFieldType ft = sec.getFieldType(fieldName);
                        if (ft.isStored() == false) {
                            continue;
                        }
                        storedFields.add(new StoredField(fieldName, ft, sec.isMetadataField(ft.name())));
                        fieldsToLoad.add(ft.name());
                    }
                }
            }
        }
        StoredFieldsSpec storedFieldsSpec = new StoredFieldsSpec(false, true, fieldsToLoad);

        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {

            }

            @Override
            public void process(HitContext hitContext) {
                Map<String, List<Object>> loadedFields = hitContext.loadedFields();
                Map<String, DocumentField> docFields = new HashMap<>();
                Map<String, DocumentField> metaFields = new HashMap<>();
                for (StoredField storedField : storedFields) {
                    if (storedField.hasValue(loadedFields)) {
                        DocumentField df = new DocumentField(storedField.name, storedField.process(loadedFields));
                        if (storedField.isMetadataField) {
                            metaFields.put(storedField.name, df);
                        } else {
                            docFields.put(storedField.name, df);
                        }
                    }
                }
                hitContext.hit().addDocumentFields(docFields, metaFields);
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return storedFieldsSpec;
            }
        };
    }

}
