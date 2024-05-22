/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.function.scalar;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.core.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.esql.core.expression.gen.script.Scripts;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.gen.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.esql.core.expression.gen.script.Scripts.PARAM;

/**
 * A {@code ScalarFunction} is a {@code Function} that takes values from some
 * operation and converts each to another value. An example would be
 * {@code ABS()}, which takes one value at a time, applies a function to the
 * value (abs) and returns a new value.
 */
public abstract class ScalarFunction extends Function {

    protected ScalarFunction(Source source) {
        super(source, emptyList());
    }

    protected ScalarFunction(Source source, List<Expression> fields) {
        super(source, fields);
    }

    // This method isn't actually used at the moment, since there is no grouping function (ie HISTOGRAM)
    // that currently results in a script being generated
    protected ScriptTemplate scriptWithGrouping(GroupingFunction grouping) {
        String template = PARAM;
        return new ScriptTemplate(processScript(template), paramsBuilder().grouping(grouping).build(), dataType());
    }

    protected String processScript(String script) {
        return formatTemplate(script);
    }

    protected String formatTemplate(String template) {
        return Scripts.formatTemplate(template);
    }
}
