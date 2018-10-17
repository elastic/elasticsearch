/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.xpack.sql.expression.gen.script.Params;
import org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Locale;

import static java.lang.String.format;

public class OrAggFilter extends AggFilter {

    public OrAggFilter(AggFilter left, AggFilter right) {
        this(left.name() + "_|_" + right.name(), left, right);
    }

    public OrAggFilter(String name, AggFilter left, AggFilter right) {
        super(name, and(left.scriptTemplate(), right.scriptTemplate()));
    }

    private static ScriptTemplate and(ScriptTemplate left, ScriptTemplate right) {
        String template = format(Locale.ROOT, "( %s ) || ( %s )", left.template(), right.template());
        Params params = new ParamsBuilder().script(left.params()).script(right.params()).build();
        return new ScriptTemplate(template, params, DataType.BOOLEAN);
    }
}
