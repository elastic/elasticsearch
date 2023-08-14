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
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappedFieldType.CollapseType;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.lucene.grouping.SinglePassGroupingCollector;
import org.elasticsearch.lucene.grouping.SinglePassGroupingCollectorWithCollapseSort;

import java.util.List;

/**
 * Context used for field collapsing
 */
public class CollapseContext {
    private final String fieldName;
    private final MappedFieldType fieldType;
    private final List<InnerHitBuilder> innerHits;

    private final Sort collapseSort;

    public CollapseContext(String fieldName, MappedFieldType fieldType, List<InnerHitBuilder> innerHits, Sort collapseSort) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.innerHits = innerHits;
        this.collapseSort = collapseSort;
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

    /** The inner hit options to expand the collapsed results **/
    public List<InnerHitBuilder> getInnerHit() {
        return innerHits;
    }

    /** The collapse sort options for selecting top docs per group **/
    public Sort getCollapseSort() {
        return collapseSort;
    }

    public SinglePassGroupingCollector<?> createTopDocs(Sort sort, int topN, int totalNumDocs, FieldDoc after) {
        if (fieldType.collapseType() == CollapseType.KEYWORD) {
            if (collapseSort == null) {
                return SinglePassGroupingCollector.createKeyword(fieldName, fieldType, sort, topN, after);
            }
            return SinglePassGroupingCollectorWithCollapseSort
                .createKeyword(fieldName, fieldType, sort, collapseSort, topN, totalNumDocs, after);
        } else if (fieldType.collapseType() == CollapseType.NUMERIC) {
            if (collapseSort == null) {
                return SinglePassGroupingCollector.createNumeric(fieldName, fieldType, sort, topN, after);
            }
            return SinglePassGroupingCollectorWithCollapseSort
                .createNumeric(fieldName, fieldType, sort, collapseSort, topN, totalNumDocs, after);
        } else {
            throw new IllegalStateException("collapse is not supported on this field type");
        }
    }
}
