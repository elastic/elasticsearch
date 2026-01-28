/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.capabilities;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;

/**
 * Interface for expressions that may be replaced based on the {@code minTransportVersion} of the clusters.
 * <p>
 *     For generic surrogation, use {@link SurrogateExpression} instead.
 * </p>
 */
public interface TransportVersionAware {
    /**
     * Returns the expression to be replaced by or {@code null} if this cannot
     * be replaced.
     */
    Expression forTransportVersion(TransportVersion minTransportVersion);
}
