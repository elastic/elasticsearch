/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.ConditionalProcessor.ConditionalOperation;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;

/**
 * Base class for conditional predicates with arbitrary number of arguments
 */
public abstract class ArbitraryConditionalFunction extends ConditionalFunction {

    private final ConditionalOperation operation;

    ArbitraryConditionalFunction(Source source, List<Expression> fields, ConditionalOperation operation) {
        super(source, fields);
        this.operation = operation;
    }

    @Override
    protected Pipe makePipe() {
        return new ConditionalPipe(source(), this, Expressions.pipe(children()), operation);
    }

    @Override
    public ScriptTemplate asScript() {
        List<ScriptTemplate> templates = new ArrayList<>();
        for (Expression ex : children()) {
            templates.add(asScript(ex));
        }

        StringJoiner template = new StringJoiner(",", "{sql}." + operation.scriptMethodName() +"([", "])");
        ParamsBuilder params = paramsBuilder();

        for (ScriptTemplate scriptTemplate : templates) {
            template.add(scriptTemplate.template());
            params.script(scriptTemplate.params());
        }

        return new ScriptTemplate(formatTemplate(template.toString()), params.build(), dataType());
    }
}
