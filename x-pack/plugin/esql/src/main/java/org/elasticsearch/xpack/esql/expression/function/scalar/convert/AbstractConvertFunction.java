/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import joptsimple.internal.Strings;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.expression.function.Warnings;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

/**
 * Base class for functions that converts a field into a function-specific type.
 */
public abstract class AbstractConvertFunction extends UnaryScalarFunction {

    // the numeric types convert functions need to handle; the other numeric types are converted upstream to one of these
    private static final List<DataType> NUMERIC_TYPES = List.of(
        DataTypes.INTEGER,
        DataTypes.LONG,
        DataTypes.UNSIGNED_LONG,
        DataTypes.DOUBLE
    );
    public static final List<DataType> STRING_TYPES = DataTypes.types().stream().filter(EsqlDataTypes::isString).toList();

    protected AbstractConvertFunction(Source source, Expression field) {
        super(source, field);
    }

    /**
     * Build the evaluator given the evaluator a multivalued field.
     */
    protected ExpressionEvaluator.Factory evaluator(ExpressionEvaluator.Factory fieldEval) {
        DataType sourceType = field().dataType();
        var factory = factories().get(sourceType);
        if (factory == null) {
            throw EsqlIllegalArgumentException.illegalDataType(sourceType);
        }
        return factory.build(fieldEval, source());
    }

    @Override
    protected final TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isType(field(), factories()::containsKey, sourceText(), null, supportedTypesNames(factories().keySet()));
    }

    public static String supportedTypesNames(Set<DataType> types) {
        List<String> supportedTypesNames = new ArrayList<>(types.size());
        HashSet<DataType> supportTypes = new HashSet<>(types);
        if (supportTypes.containsAll(NUMERIC_TYPES)) {
            supportedTypesNames.add("numeric");
            NUMERIC_TYPES.forEach(supportTypes::remove);
        }

        if (types.containsAll(STRING_TYPES)) {
            supportedTypesNames.add("string");
            STRING_TYPES.forEach(supportTypes::remove);
        }

        supportTypes.forEach(t -> supportedTypesNames.add(t.name().toLowerCase(Locale.ROOT)));
        supportedTypesNames.sort(String::compareTo);
        return Strings.join(supportedTypesNames, " or ");
    }

    @FunctionalInterface
    interface BuildFactory {
        ExpressionEvaluator.Factory build(ExpressionEvaluator.Factory field, Source source);
    }

    protected abstract Map<DataType, BuildFactory> factories();

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        return evaluator(toEvaluator.apply(field()));
    }

    public abstract static class AbstractEvaluator implements EvalOperator.ExpressionEvaluator {

        private static final Log logger = LogFactory.getLog(AbstractEvaluator.class);

        protected final DriverContext driverContext;
        private final EvalOperator.ExpressionEvaluator fieldEvaluator;
        private final Warnings warnings;

        protected AbstractEvaluator(DriverContext driverContext, EvalOperator.ExpressionEvaluator field, Source source) {
            this.driverContext = driverContext;
            this.fieldEvaluator = field;
            this.warnings = new Warnings(source);
        }

        protected abstract String name();

        /**
         * Called when evaluating a {@link Block} that contains null values.
         * @return the returned Block has its own reference and the caller is responsible for releasing it.
         */
        protected abstract Block evalBlock(Block b);

        /**
         * Called when evaluating a {@link Block} that does not contain null values.
         * @return the returned Block has its own reference and the caller is responsible for releasing it.
         */
        protected abstract Block evalVector(Vector v);

        @Override
        public final Block eval(Page page) {
            try (Block block = fieldEvaluator.eval(page)) {
                Vector vector = block.asVector();
                return vector == null ? evalBlock(block) : evalVector(vector);
            }
        }

        protected final void registerException(Exception exception) {
            logger.trace("conversion failure", exception);
            warnings.registerException(exception);
        }

        @Override
        public final String toString() {
            return name() + "Evaluator[field=" + fieldEvaluator + "]";
        }

        @Override
        public void close() {
            // TODO toString allocates - we should probably check breakers there too
            Releasables.closeExpectNoException(fieldEvaluator);
        }
    }
}
