/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.testfunction;

import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.plugin.EsqlFunctionProvider;

import java.util.Collection;
import java.util.List;

/**
 * SPI extension class that provides the Abs3 function.
 * This class is discovered via META-INF/services/org.elasticsearch.xpack.esql.plugin.EsqlFunctionProvider
 */
public class Abs3FunctionProvider implements EsqlFunctionProvider {

    @Override
    public Collection<FunctionDefinition> getEsqlFunctions() {
        return List.of(EsqlFunctionRegistry.createRuntimeDef(Abs3.class, Abs3::new, "abs3"));
    }
}
