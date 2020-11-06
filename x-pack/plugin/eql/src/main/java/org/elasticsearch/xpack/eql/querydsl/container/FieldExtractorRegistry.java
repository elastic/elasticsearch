/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.querydsl.container;

import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.ql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.ConstantInput;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class used for creating (and reusing) Field extractors for
 * given Attributes.
 */
public class FieldExtractorRegistry {

    private final Map<String, FieldExtraction> cache = new HashMap<>();
    
    public FieldExtraction fieldExtraction(Expression expression) {
        return cache.computeIfAbsent(Expressions.id(expression), k -> createFieldExtractionFor(expression));
    }

    private FieldExtraction createFieldExtractionFor(Expression expression) {
        if (expression instanceof FieldAttribute) {
            FieldAttribute fa = (FieldAttribute) expression;
            if (fa.isNested()) {
                throw new UnsupportedOperationException("Nested not yet supported");
            }
            return topHitFieldExtractor(fa);
        }
        if (expression.foldable()) {
            return new ComputedRef(new ConstantInput(expression.source(), expression, expression.fold()));
        }

        throw new EqlIllegalArgumentException("Unsupported expression [{}]", expression);
    }
    
    private FieldExtraction topHitFieldExtractor(FieldAttribute fieldAttr) {
        FieldAttribute actualField = fieldAttr;
        FieldAttribute rootField = fieldAttr;
        StringBuilder fullFieldName = new StringBuilder(fieldAttr.field().getName());
        
        // Only if the field is not an alias (in which case it will be taken out from docvalue_fields if it's isAggregatable()),
        // go up the tree of parents until a non-object (and non-nested) type of field is found and use that specific parent
        // as the field to extract data from, from _source. We do it like this because sub-fields are not in the _source, only
        // the root field to which those sub-fields belong to, are. Instead of "text_field.keyword_subfield" for _source extraction,
        // we use "text_field", because there is no source for "keyword_subfield".
        /*
         *    "text_field": {
         *       "type": "text",
         *       "fields": {
         *         "keyword_subfield": {
         *           "type": "keyword"
         *         }
         *       }
         *     }
         */
        if (fieldAttr.field().isAlias() == false) {
            while (actualField.parent() != null
                    && actualField.parent().field().getDataType() != DataTypes.OBJECT
                    && actualField.parent().field().getDataType() != DataTypes.NESTED
                    && actualField.field().getDataType().hasDocValues() == false) {
                actualField = actualField.parent();
            }
        }
        while (rootField.parent() != null) {
            fullFieldName.insert(0, ".").insert(0, rootField.parent().field().getName());
            rootField = rootField.parent();
        }

        return new SearchHitFieldRef(actualField.name(), fullFieldName.toString(), fieldAttr.field().getDataType(),
                                     fieldAttr.field().isAggregatable(), fieldAttr.field().isAlias());
    }
}
