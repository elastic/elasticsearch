/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.core.expression.Expression;

/**
 * Marker interface to identify classes of functions that operate on the {code @timestamp} field of an index.
 * Implementations of this interface need to expect the associated {@code Attribute} to be passed as the following argument after the
 * {@code Source} one, which is always the first one.
 */
public interface TimestampAware {
    Expression timestamp();
}
