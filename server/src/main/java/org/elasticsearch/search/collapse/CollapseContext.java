/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.collapse;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Sort;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappedFieldType.CollapseType;
import org.elasticsearch.lucene.grouping.SinglePassGroupingCollector;

/**
 * Context used for field collapsing
 */
public class CollapseContext {
    private final String fieldName;
    private final MappedFieldType fieldType;

    public CollapseContext(String fieldName, MappedFieldType fieldType) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
    }

    /**
     * The requested field name to collapse on.
     */
    public String getFieldName() {
        return fieldName;
    }

    /** The field type used for collapsing **/
    public MappedFieldType getFieldType() {
        return fieldType;
    }

    public SinglePassGroupingCollector<?> createTopDocs(Sort sort, int topN, FieldDoc after) {
        if (fieldType.collapseType() == CollapseType.KEYWORD) {
            return SinglePassGroupingCollector.createKeyword(fieldName, fieldType, sort, topN, after);
        } else if (fieldType.collapseType() == CollapseType.NUMERIC) {
            return SinglePassGroupingCollector.createNumeric(fieldName, fieldType, sort, topN, after);
        } else {
            throw new IllegalStateException("collapse is not supported on this field type");
        }
    }
}
