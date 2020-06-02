/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.expression.function.aggregate;

import org.elasticsearch.xpack.ql.expression.Expression;

import java.util.List;

/**
 * Marker type for compound aggregates, that is an aggregate that provides multiple values (like Stats or Matrix)
 */
public interface CompoundAggregate {

    Expression field();

    List<Expression> arguments();
}
