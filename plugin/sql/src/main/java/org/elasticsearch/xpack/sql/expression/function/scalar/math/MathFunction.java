/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinitions;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.UnaryProcessorDefinition;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Locale;
import java.util.Objects;

import static java.lang.String.format;

public abstract class MathFunction extends UnaryScalarFunction {

    protected MathFunction(Location location) {
        super(location);
    }

    protected MathFunction(Location location, Expression field) {
        super(location, field);
    }

    @Override
    public boolean foldable() {
        return field().foldable();
    }

    @Override
    public Object fold() {
        return operation().apply(field().fold());
    }

    @Override
    protected String formatScript(String template) {
        return super.formatScript(format(Locale.ROOT, "Math.%s(%s)", mathFunction(), template));
    }

    protected String mathFunction() {
        return getClass().getSimpleName().toLowerCase(Locale.ROOT);
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }
    
    @Override
    protected TypeResolution resolveType() {
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }

        return field().dataType().isNumeric() ? TypeResolution.TYPE_RESOLVED
                : new TypeResolution("'%s' requires a numeric type, received %s", operation(), field().dataType().esType);
    }

    @Override
    protected final ProcessorDefinition makeProcessorDefinition() {
        return new UnaryProcessorDefinition(location(), this,
            ProcessorDefinitions.toProcessorDefinition(field()), new MathProcessor(operation()));
    }

    protected abstract MathOperation operation();

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        MathFunction other = (MathFunction) obj;
        return Objects.equals(other.field(), field());
    }

    @Override
    public int hashCode() {
        return Objects.hash(field());
    }
}