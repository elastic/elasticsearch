/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.TypedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;

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
 *         referred to here as ExactSubfield. This that this is related to normal ES|QL predicates,
 *         not the full-text search provided by the MATCH and QSTR functions, which are pushed down separately.</li>
 * </ol>
 */
public interface LucenePushdownPredicates {
    /**
     * For TEXT fields, we need to check if the field has a subfield of type KEYWORD that can be used instead.
     */
    boolean hasExactSubfield(FieldAttribute attr);

    /**
     * For pushing down TopN and for pushing down filters with SingleValueQuery,
     * we need to check if the field is indexed and has doc values.
     */
    boolean isIndexedAndHasDocValues(FieldAttribute attr);

    /**
     * For pushing down filters when multi-value results are allowed (spatial functions like ST_INTERSECTS),
     * we only need to know if the field is indexed.
     */
    boolean isIndexed(FieldAttribute attr);

    boolean canUseEqualityOnSyntheticSourceDelegate(FieldAttribute attr, String value);

    /**
     * We see fields as pushable if either they are aggregatable or they are indexed.
     * This covers non-indexed cases like <code>AbstractScriptFieldType</code> which hard-coded <code>isAggregatable</code> to true,
     * as well as normal <code>FieldAttribute</code>'s which can only be pushed down if they are indexed.
     * The reason we don't just rely entirely on <code>isAggregatable</code> is because this is often false for normal fields, and could
     * also differ from node to node, and we can physically plan each node separately, allowing Lucene pushdown on the nodes that
     * support it, and relying on the compute engine for the nodes that do not.
     */
    default boolean isPushableFieldAttribute(Expression exp) {
        if (exp instanceof FieldAttribute fa && fa.getExactInfo().hasExact() && isIndexedAndHasDocValues(fa)) {
            return fa.dataType() != DataType.TEXT || hasExactSubfield(fa);
        }
        return false;
    }

    static boolean isPushableTextFieldAttribute(Expression exp) {
        return exp instanceof FieldAttribute fa && fa.dataType() == DataType.TEXT;
    }

    static boolean isPushableMetadataAttribute(Expression exp) {
        return exp instanceof MetadataAttribute ma && (ma.searchable() || ma.name().equals(MetadataAttribute.SCORE));
    }

    default boolean isPushableAttribute(Expression exp) {
        return isPushableFieldAttribute(exp) || isPushableMetadataAttribute(exp);
    }

    static TypedAttribute checkIsPushableAttribute(Expression e) {
        Check.isTrue(
            e instanceof FieldAttribute || e instanceof MetadataAttribute,
            "Expected a FieldAttribute or MetadataAttribute but received [{}]",
            e
        );
        return (TypedAttribute) e;
    }

    static FieldAttribute checkIsFieldAttribute(Expression e) {
        Check.isTrue(e instanceof FieldAttribute, "Expected a FieldAttribute but received [{}] of type [{}]", e, e.getClass());
        return (FieldAttribute) e;
    }

    static String pushableAttributeName(TypedAttribute attribute) {
        return attribute instanceof FieldAttribute fa
            ? fa.exactAttribute().name() // equality should always be against an exact match (which is important for strings)
            : attribute.name();
    }

    /**
     * Extract the real field name from a MultiTypeEsField, limit to MultiTypeEsField that has date_nanos type only.
     *
     * For example, the name of a MultiTypeEsField can be $$myfield$converted_to$date_nanos, and the real field name extract from the
     * MultiTypeEsField is myfield, this method return myfield given a MultiTypeEsField.
     *
     * If the real field name is found, and the original field data types contain only date and date_nanos types, return the real field
     * name, so that the real field name will be used to check for eligibility of being pushed down, and the real field name will be used
     * in the push down query, instead of the name of the MultiTypeEsField, which should not match any field in an index.
     *
     * This method can be extended to support the other data types in the future if there is a need.
     */
    static String extractFieldNameFromMultiTypeEsField(TypedAttribute attribute) {
        if (EsqlCapabilities.Cap.IMPLICIT_CASTING_DATE_AND_DATE_NANOS.isEnabled()
            && attribute instanceof FieldAttribute fa
            && fa.field() instanceof MultiTypeEsField multiTypeEsField
            && fa.dataType() == DATE_NANOS
            &&  // limit to casting to date_nanos only
            mixedDateAndDateNanosOnly(multiTypeEsField, DataType::isMillisOrNanos) // limit to mixed date and date_nanos only
        ) {
            return fa.fieldName();
        }
        return null;
    }

    /**
     * Check if the original field types in a MultiTypeEsField satisfy the required data types defined in the predicate.
     */
    private static boolean mixedDateAndDateNanosOnly(MultiTypeEsField multiTypeEsField, Predicate<DataType> predicate) {
        Map<String, Expression> indexToConversionExpressions = multiTypeEsField.getIndexToConversionExpressions();
        for (Map.Entry<String, Expression> entry : indexToConversionExpressions.entrySet()) {
            Expression conversionFunction = entry.getValue();
            if (conversionFunction instanceof AbstractConvertFunction abstractConvertFunction
                && predicate.test(abstractConvertFunction.field().dataType()) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * The default implementation of this has no access to SearchStats, so it can only make decisions based on the FieldAttribute itself.
     * In particular, it assumes TEXT fields have no exact subfields (underlying keyword field),
     * and that isAggregatable means indexed and has hasDocValues.
     */
    LucenePushdownPredicates DEFAULT = new LucenePushdownPredicates() {
        @Override
        public boolean hasExactSubfield(FieldAttribute attr) {
            return false;
        }

        @Override
        public boolean isIndexedAndHasDocValues(FieldAttribute attr) {
            // Is the FieldType.isAggregatable() check correct here? In FieldType isAggregatable usually only means hasDocValues
            return attr.field().isAggregatable();
        }

        @Override
        public boolean isIndexed(FieldAttribute attr) {
            // TODO: This is the original behaviour, but is it correct? In FieldType isAggregatable usually only means hasDocValues
            return attr.field().isAggregatable();
        }

        @Override
        public boolean canUseEqualityOnSyntheticSourceDelegate(FieldAttribute attr, String value) {
            return false;
        }
    };

    /**
     * If we have access to SearchStats over a collection of shards, we can make more fine-grained decisions about what can be pushed down.
     * This should open up more opportunities for lucene pushdown.
     */
    static LucenePushdownPredicates from(SearchStats stats) {
        return new LucenePushdownPredicates() {
            @Override
            public boolean hasExactSubfield(FieldAttribute attr) {
                return stats.hasExactSubfield(attr.name());
            }

            @Override
            public boolean isIndexedAndHasDocValues(FieldAttribute attr) {
                // If this is a MultiTypeEsField cast to date_nanos, make it eligible for being pushed down by checking the real
                // field name against SearchStats
                String fieldNameFromMultiTypeEsField = LucenePushdownPredicates.extractFieldNameFromMultiTypeEsField(attr);
                String name = fieldNameFromMultiTypeEsField != null ? fieldNameFromMultiTypeEsField : attr.name();
                // We still consider the value of isAggregatable here, because some fields like ScriptFieldTypes are always aggregatable
                // But this could hide issues with fields that are not indexed but are aggregatable
                // This is the original behaviour for ES|QL, but is it correct?
                return attr.field().isAggregatable() || stats.isIndexed(name) && stats.hasDocValues(name);
            }

            @Override
            public boolean isIndexed(FieldAttribute attr) {
                return stats.isIndexed(attr.name());
            }

            @Override
            public boolean canUseEqualityOnSyntheticSourceDelegate(FieldAttribute attr, String value) {
                return stats.canUseEqualityOnSyntheticSourceDelegate(attr.field().getName(), value);
            }
        };
    }
}
