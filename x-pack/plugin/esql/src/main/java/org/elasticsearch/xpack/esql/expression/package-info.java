/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * {@link org.elasticsearch.xpack.esql.core.expression.Expression Expressions} process values
 * to make more values. There are two kinds:
 * <ul>
 *     <li>
 *         {@link org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction scalars}
 *         take a single row as input and produce a value as output.
 *     </li>
 *     <li>
 *         {@link org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction aggregates}
 *         take many rows as input and produce some values as output.
 *     </li>
 * </ul>
 */
package org.elasticsearch.xpack.esql.expression;
