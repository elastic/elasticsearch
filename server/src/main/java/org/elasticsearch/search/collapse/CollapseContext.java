/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.collapse;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Sort;
import org.elasticsearch.index.mapper.MappedField;
import org.elasticsearch.index.mapper.MappedFieldType.CollapseType;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.lucene.grouping.SinglePassGroupingCollector;

import java.util.List;

/**
 * Context used for field collapsing
 */
public class CollapseContext {
    private final String fieldName;
    private final MappedField mappedField;
    private final List<InnerHitBuilder> innerHits;

    public CollapseContext(String fieldName, MappedField mappedField, List<InnerHitBuilder> innerHits) {
        this.fieldName = fieldName;
        this.mappedField = mappedField;
        this.innerHits = innerHits;
    }

    /**
     * The requested field name to collapse on.
     */
    public String getFieldName() {
        return fieldName;
    }

    /** The mapped field used for collapsing **/
    public MappedField getMappedField() {
        return mappedField;
    }

    /** The inner hit options to expand the collapsed results **/
    public List<InnerHitBuilder> getInnerHit() {
        return innerHits;
    }

    public SinglePassGroupingCollector<?> createTopDocs(Sort sort, int topN, FieldDoc after) {
        if (mappedField.collapseType() == CollapseType.KEYWORD) {
            return SinglePassGroupingCollector.createKeyword(fieldName, mappedField, sort, topN, after);
        } else if (mappedField.collapseType() == CollapseType.NUMERIC) {
            return SinglePassGroupingCollector.createNumeric(fieldName, mappedField, sort, topN, after);
        } else {
            throw new IllegalStateException("collapse is not supported on this field type");
        }
    }
}
