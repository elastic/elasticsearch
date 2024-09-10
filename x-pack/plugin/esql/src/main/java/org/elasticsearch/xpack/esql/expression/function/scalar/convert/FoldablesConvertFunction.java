/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.Validatable;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.isString;
import static org.elasticsearch.xpack.esql.expression.Validations.isFoldable;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.foldToTemporalAmount;

/**
 * Base class for functions that converts a constant into an interval type - DATE_PERIOD or TIME_DURATION.
 * The functions will be folded at the end of LogicalPlanOptimizer by the coordinator, it does not reach data node.
 */
public abstract class FoldablesConvertFunction extends AbstractConvertFunction implements Validatable {

    protected FoldablesConvertFunction(Source source, Expression field) {
        super(source, field);
    }

    @Override
    public final void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public final String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    protected final TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isType(
            field(),
            dt -> isString(dt) || dt == dataType(),
            sourceText(),
            null,
            false,
            dataType().typeName().toLowerCase(Locale.ROOT) + " or string"
        );
    }

    @Override
    protected final Map<DataType, BuildFactory> factories() {
        // TODO if a union type field is provided as an input, the correct error message is not shown, #112668 is a follow up
        return Map.of();
    }

    @Override
    public final Object fold() {
        return foldToTemporalAmount(field(), sourceText(), dataType());
    }

    @Override
    public final void validate(Failures failures) {
        failures.add(isFoldable(field(), sourceText(), null));
    }
}
