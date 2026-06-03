/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

/**
 * Marker interface indicating that a function needs a Configuration object as its last parameter.
 * <p>
 *     Extend {@link org.elasticsearch.xpack.esql.expression.function.scalar.EsqlConfigurationFunction} instead if possible.
 *     It automatically takes care of:
 * </p>
 * <ul>
 *     <li>The Configuration field</li>
 *     <li>HashCode</li>
 *     <li>Equals</li>
 * </ul>
 * <p>
 *     If overridden directly, take a look at {@link org.elasticsearch.xpack.esql.expression.function.scalar.EsqlConfigurationFunction}
 *     and add or update the required methods.
 * </p>
 */
public interface ConfigurationFunction {

}
