/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * ES|QL Overview and Documentation Links
 *
 * <h2>Major Components</h2>
 * <ul>
 * <li>{@link org.elasticsearch.compute} - The compute engine drives query execution
 * <ul>
 *     <li>{@link org.elasticsearch.compute.data.Block} - fundamental unit of data.  Operations vectorize over blocks.</li>
 *     <li>{@link org.elasticsearch.compute.data.Page} - Data is broken up into pages (which are collections of blocks) to
 *     manage size in memory</li>
 * </ul>
 * </li>
 * <li>{@link org.elasticsearch.xpack.esql.core} - Core Utility Classes
 * <ul>
 *     <li>{@link org.elasticsearch.xpack.esql.core.type.DataType} - ES|QL is a typed language, and all the supported data types
 *     are listed in this collection.</li>
 *     <li>{@link org.elasticsearch.xpack.esql.core.expression.Expression} - Expression is the basis for all functions in ES|QL,
 *     but see also {@link org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper}</li>
 * </ul>
 * </li>
 * <li> org.elasticsearch.compute.gen - ES|QL generates code for evaluators, which are type-specific implementations of
 * functions, designed to run over a {@link org.elasticsearch.compute.data.Block} </li>
 * <li>{@link org.elasticsearch.xpack.esql.session.EsqlSession} - manages state across a query</li>
 * <li>{@link org.elasticsearch.xpack.esql.expression.function.scalar} - Guide to writing scalar functions</li>
 * <li>{@link org.elasticsearch.xpack.esql.analysis.Analyzer} - The first step in query processing</li>
 * <li>{@link org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer} - Coordinator level logical optimizations</li>
 * <li>{@link org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer} - Data node level logical optimizations</li>
 * <li>{@link org.elasticsearch.xpack.esql.action.RestEsqlQueryAction} - REST API entry point</li>
 * </ul>
 */

package org.elasticsearch.xpack.esql;
