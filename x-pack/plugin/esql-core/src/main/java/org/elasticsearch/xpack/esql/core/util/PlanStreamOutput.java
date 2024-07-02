/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;

import java.io.IOException;

/**
 * Interface for streams that can serialize plan components. This exists so
 * ESQL proper can expose streaming capability to ESQL-core. If the world is kind
 * and just we'll remove this when we flatten everything from ESQL-core into
 * ESQL proper.
 */
public interface PlanStreamOutput {
    /**
     * Write an {@link Expression} to the stream. This will soon be replaced with
     * {@link StreamOutput#writeNamedWriteable}.
     */
    void writeExpression(Expression expression) throws IOException;
}
