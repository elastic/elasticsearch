/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.xpack.sql.expression.gen.script.Scripts;

public class OrAggFilter extends AggFilter {

    public OrAggFilter(AggFilter left, AggFilter right) {
        this(left.name() + "_|_" + right.name(), left, right);
    }

    public OrAggFilter(String name, AggFilter left, AggFilter right) {
        super(name, Scripts.or(left.scriptTemplate(), right.scriptTemplate()));
    }
}
