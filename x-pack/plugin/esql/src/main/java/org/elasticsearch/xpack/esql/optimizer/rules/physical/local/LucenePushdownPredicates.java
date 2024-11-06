/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.stats.SearchStats;

/**
 * When deciding if a filter or topN can be pushed down to Lucene, we need to check a few things on the field.
 * Exactly what is checked depends on the type of field and the query. For example, we have the following possible combinations:
 * <ol>
 *     <li>A normal filter on a normal field will be pushed down using SingleValueQuery to remove multi-valued results,
 *         and this requires knowing if the field is indexed and has doc-values.</li>
 *     <li>A filter using a spatial function will allow multi-valued fields and we only need to know if the field is indexed,
 *         and do not need doc values.</li>
 *     <li>A TopN will be pushed down if the field is indexed and has doc values.</li>
 *     <li>Filters with TEXT fields can only be pushed down if the TEXT field has a nested KEYWORD field,
 *         referred to here as IdenticalDelegate. This that this is related to normal ES|QL predicates,
 *         not the full-text search provided by the MATCH and QSTR functions, which are pushed down separately.</li>
 * </ol>
 */
public interface LucenePushdownPredicates {
    /**
     * For TEXT fields, we need to check if the field has an identical delegate.
     */
    boolean hasIdenticalDelegate(FieldAttribute attr);

    /**
     * For pushing down TopN and for pushing down filters with SingleValueQuery,
     * we need to check if the field is indexed and has doc values.
     */
    boolean isIndexedAndDocValues(FieldAttribute attr);

    /**
     * For pushing down filters when multi-value results are allowed (spatial functions like ST_INTERSECTS),
     * we only need to know if the field is indexed.
     */
    boolean isIndexed(FieldAttribute attr);

    LucenePushdownPredicates DEFAULT = new LucenePushdownPredicates() {
        @Override
        public boolean hasIdenticalDelegate(FieldAttribute attr) {
            return false;
        }

        @Override
        public boolean isIndexedAndDocValues(FieldAttribute attr) {
            // Is the FieldType.isAggregatable() check correct here? In FieldType isAggregatable usually only means hasDocValues
            return attr.field().isAggregatable();
        }

        @Override
        public boolean isIndexed(FieldAttribute attr) {
            // TODO: This is the original behaviour, but is it correct? In FieldType isAggregatable usually only means hasDocValues
            return attr.field().isAggregatable();
        }
    };

    static LucenePushdownPredicates from(SearchStats stats) {
        return new LucenePushdownPredicates() {
            @Override
            public boolean hasIdenticalDelegate(FieldAttribute attr) {
                return stats.hasIdenticalDelegate(attr.name());
            }

            @Override
            public boolean isIndexedAndDocValues(FieldAttribute attr) {
                // We still consider the value of isAggregatable here, because some fields like ScriptFieldTypes are always aggregatable
                // But this could hide issues with fields that are not indexed but are aggregatable
                // This is the original behaviour for ES|QL, but is it correct?
                return attr.field().isAggregatable() || stats.isIndexed(attr.name()) && stats.hasDocValues(attr.name());
            }

            @Override
            public boolean isIndexed(FieldAttribute attr) {
                return stats.isIndexed(attr.name());
            }
        };
    }
}
