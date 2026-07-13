/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.xpack.ql.expression.gen.script.Scripts;

public class OrAggFilter extends AggFilter {

    public OrAggFilter(AggFilter left, AggFilter right) {
        this(left.name() + "_|_" + right.name(), left, right);
    }

    public OrAggFilter(String name, AggFilter left, AggFilter right) {
        super(name, Scripts.or(left.scriptTemplate(), right.scriptTemplate()));
    }
}
