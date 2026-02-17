/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.operator.SourceOperator;

/**
 * Functional interface for creating source operator factories.
 *
 * <p>This is the extension point for plugins that need custom operator logic
 * beyond what the generic AsyncExternalSourceOperatorFactory provides.
 *
 * <p>Implementations receive a {@link SourceOperatorContext} containing all
 * necessary information to create the operator factory.
 */
@FunctionalInterface
public interface SourceOperatorFactoryProvider {

    SourceOperator.SourceOperatorFactory create(SourceOperatorContext context);
}
