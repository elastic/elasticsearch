/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.querydsl.container;

import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.expression.OptionalMissingAttribute;
import org.elasticsearch.xpack.ql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.ConstantInput;

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

    public FieldExtraction compositeKeyExtraction(Expression expression) {
        return cache.computeIfAbsent(Expressions.id(expression), k -> createKeyExtractionFor(expression));
    }

    private FieldExtraction createKeyExtractionFor(Expression expression) {
        if (expression instanceof FieldAttribute fieldAttribute) {
            FieldAttribute fa = fieldAttribute.exactAttribute();
            if (fa.isNested()) {
                throw new UnsupportedOperationException("Nested not yet supported");
            }
            return new CompositeAggRef(fa.name());
        }
        if (expression instanceof OptionalMissingAttribute) {
            return new ComputedRef(new ConstantInput(expression.source(), expression, null));
        }
        throw new EqlIllegalArgumentException("Unsupported expression [{}]", expression);
    }

    private FieldExtraction createFieldExtractionFor(Expression expression) {
        if (expression instanceof FieldAttribute fieldAttribute) {
            FieldAttribute fa = fieldAttribute.exactAttribute();
            if (fa.isNested()) {
                throw new UnsupportedOperationException("Nested not yet supported");
            }
            return topHitFieldExtractor(fa);
        }
        if (expression instanceof OptionalMissingAttribute) {
            return new ComputedRef(new ConstantInput(expression.source(), expression, null));
        }
        if (expression.foldable()) {
            return new ComputedRef(new ConstantInput(expression.source(), expression, expression.fold()));
        }

        throw new EqlIllegalArgumentException("Unsupported expression [{}]", expression);
    }

    private FieldExtraction topHitFieldExtractor(FieldAttribute fieldAttr) {
        return new SearchHitFieldRef(fieldAttr.name(), fieldAttr.field().getDataType(), fieldAttr.field().isAlias());
    }
}
