/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;

import java.util.Collection;

/**
 * Interface for plugins that provide ES|QL functions.
 * <p>
 * Plugins implementing this interface can register custom ES|QL functions
 * that will be available in ES|QL queries. This enables external modules
 * to extend ES|QL functionality without modifying the core ES|QL codebase.
 * <p>
 * Example usage:
 * <pre>{@code
 * public class MyFunctionPlugin extends Plugin implements EsqlFunctionProvider {
 *     @Override
 *     public Collection<FunctionDefinition> getEsqlFunctions() {
 *         return List.of(
 *             EsqlFunctionRegistry.createRuntimeDef(MyFunction.class, MyFunction::new, "my_function")
 *         );
 *     }
 * }
 * }</pre>
 *
 * @see org.elasticsearch.xpack.esql.expression.function.FunctionDefinition
 * @see org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry
 */
public interface EsqlFunctionProvider {

    /**
     * Returns the ES|QL functions provided by this plugin.
     * <p>
     * The returned collection should contain function definitions created via
     * {@link org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry#createRuntimeDef}
     * or similar factory methods.
     * </p>
     *
     * @return a collection of function definitions, never null
     */
    Collection<FunctionDefinition> getEsqlFunctions();
}
