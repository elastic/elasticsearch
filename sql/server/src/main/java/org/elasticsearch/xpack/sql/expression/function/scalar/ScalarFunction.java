/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;

import java.util.List;

import static java.util.Collections.emptyList;

public abstract class ScalarFunction extends Function {

    private ProcessorDefinition lazyProcessor = null;

    protected ScalarFunction(Location location) {
        super(location, emptyList());
    }

    protected ScalarFunction(Location location, List<Expression> fields) {
        super(location, fields);
    }

    @Override
    public abstract ScalarFunctionAttribute toAttribute();

    protected abstract ScriptTemplate asScript();

    public ProcessorDefinition asProcessorDefinition() {
        if (lazyProcessor == null) {
            lazyProcessor = makeProcessorDefinition();
        }
        return lazyProcessor;
    }

    protected abstract ProcessorDefinition makeProcessorDefinition();

    // used if the function is monotonic and thus does not have to be computed for ordering purposes
    // null means the script needs to be used; expression the field/expression to be used instead
    public Expression orderBy() {
        return null;
    }
}