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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * All the required context to pull a field from the doc values.
 * This contains:
 * <ul>
 *   <li>a list of field names and its format.
 * </ul>
 */
public class FetchDocValuesContext {

    private final List<FieldAndFormat> fields = new ArrayList<>();

    /**
     * Create a new FetchDocValuesContext using the provided input list.
     * Field patterns containing wildcards are resolved and unmapped fields are filtered out.
     */
    public FetchDocValuesContext(SearchExecutionContext searchExecutionContext, List<FieldAndFormat> fieldPatterns) {
        for (FieldAndFormat field : fieldPatterns) {
            Collection<String> fieldNames = searchExecutionContext.getMatchingFieldNames(field.field);
            for (String fieldName : fieldNames) {
                fields.add(new FieldAndFormat(fieldName, field.format, field.includeUnmapped));
            }
        }

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
    public List<FieldAndFormat> fields() {
        return this.fields;
    }
}
