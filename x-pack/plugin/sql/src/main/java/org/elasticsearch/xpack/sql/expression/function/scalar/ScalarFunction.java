/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.Params;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;

import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate.formatTemplate;

/**
 * A {@code ScalarFunction} is a {@code Function} that takes values from some
 * operation and converts each to another value. An example would be
 * {@code ABS()}, which takes one value at a time, applies a function to the
 * value (abs) and returns a new value.
 */
public abstract class ScalarFunction extends Function {

    private ScalarFunctionAttribute lazyAttribute = null;
    private ProcessorDefinition lazyProcessor = null;


    protected ScalarFunction(Location location) {
        super(location, emptyList());
    }

    protected ScalarFunction(Location location, List<Expression> fields) {
        super(location, fields);
    }

    @Override
    public final ScalarFunctionAttribute toAttribute() {
        if (lazyAttribute == null) {
            lazyAttribute = new ScalarFunctionAttribute(location(), name(), dataType(), id(), functionId(), asScript(), orderBy(),
                asProcessorDefinition());
        }
        return lazyAttribute;
    }

    public abstract ScriptTemplate asScript();

    // utility methods for creating the actual scripts
    protected ScriptTemplate asScript(Expression exp) {
        if (exp.foldable()) {
            return asScriptFromFoldable(exp);
        }

        Attribute attr = Expressions.attribute(exp);
        if (attr != null) {
            if (attr instanceof ScalarFunctionAttribute) {
                return asScriptFrom((ScalarFunctionAttribute) attr);
            }
            if (attr instanceof AggregateFunctionAttribute) {
                return asScriptFrom((AggregateFunctionAttribute) attr);
            }
            if (attr instanceof FieldAttribute) {
                return asScriptFrom((FieldAttribute) attr);
            }
        }
        throw new SqlIllegalArgumentException("Cannot evaluate script for expression {}", exp);
    }

    protected ScriptTemplate asScriptFrom(ScalarFunctionAttribute scalar) {
        ScriptTemplate nested = scalar.script();
        Params p = paramsBuilder().script(nested.params()).build();
        return new ScriptTemplate(formatScript(nested.template()), p, dataType());
    }

    protected ScriptTemplate asScriptFromFoldable(Expression foldable) {
        return new ScriptTemplate(formatScript("{}"),
                paramsBuilder().variable(foldable.fold()).build(),
                foldable.dataType());
    }

    protected ScriptTemplate asScriptFrom(FieldAttribute field) {
        return new ScriptTemplate(formatScript("doc[{}].value"),
                paramsBuilder().variable(field.name()).build(),
                field.dataType());
    }

    protected ScriptTemplate asScriptFrom(AggregateFunctionAttribute aggregate) {
        return new ScriptTemplate(formatScript("{}"),
                paramsBuilder().agg(aggregate).build(),
                aggregate.dataType());
    }

    protected String formatScript(String scriptTemplate) {
        return formatTemplate(scriptTemplate);
    }

    public ProcessorDefinition asProcessorDefinition() {
        if (lazyProcessor == null) {
            lazyProcessor = makeProcessorDefinition();
        }
        return lazyProcessor;
    }

    protected abstract ProcessorDefinition makeProcessorDefinition();

    // used if the function is monotonic and thus does not have to be computed for ordering purposes
    // null means the script needs to be used; expression means the field/expression to be used instead
    public Expression orderBy() {
        return null;
    }
}
