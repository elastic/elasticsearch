/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * All the required context to pull a field from the doc values.
 * This contains:
 * <ul>
 *   <li>a list of field names and its format.
 * </ul>
 */
public class FetchDocValuesContext {

    private final Collection<FieldAndFormat> fields;

    /**
     * Create a new FetchDocValuesContext using the provided input list.
     * Field patterns containing wildcards are resolved and unmapped fields are filtered out.
     */
    public FetchDocValuesContext(SearchExecutionContext searchExecutionContext, List<FieldAndFormat> fieldPatterns) {
        // Use Linked HashMap to reserve the fetching order
        final Map<String, FieldAndFormat> fieldToFormats = new LinkedHashMap<>();
        for (FieldAndFormat field : fieldPatterns) {
            Collection<String> fieldNames = searchExecutionContext.getMatchingFieldNames(field.field);
            for (String fieldName : fieldNames) {
                // the last matching field wins
                fieldToFormats.put(fieldName, new FieldAndFormat(fieldName, field.format, field.includeUnmapped));
            }
        }
        this.fields = fieldToFormats.values();
        int maxAllowedDocvalueFields = searchExecutionContext.getIndexSettings().getMaxDocvalueFields();
        if (fields.size() > maxAllowedDocvalueFields) {
            throw new IllegalArgumentException(
                "Trying to retrieve too many docvalue_fields. Must be less than or equal to: ["
                    + maxAllowedDocvalueFields
                    + "] but was ["
                    + fields.size()
                    + "]. This limit can be set by changing the ["
                    + IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.getKey()
                    + "] index level setting."
            );
        }
    }

    /**
     * Returns the required docvalue fields.
     */
    public Collection<FieldAndFormat> fields() {
        return this.fields;
    }
}
