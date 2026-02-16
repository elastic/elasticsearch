/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.testfunction;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.plugin.EsqlFunctionProvider;

import java.util.Collection;
import java.util.List;

/**
 * Test plugin that demonstrates ES|QL function registration from an external module.
 * <p>
 * This plugin registers the {@link Abs3} function which uses runtime bytecode
 * generation instead of compile-time code generation.
 * </p>
 * <p>
 * The plugin implements {@link EsqlFunctionProvider} to provide functions to the
 * ES|QL engine. It extends the x-pack-esql plugin (declared in build.gradle).
 * </p>
 */
public class TestFunctionPlugin extends Plugin implements EsqlFunctionProvider {

    @Override
    public Collection<FunctionDefinition> getEsqlFunctions() {
        return List.of(EsqlFunctionRegistry.createRuntimeDef(Abs3.class, Abs3::new, "abs3"));
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(Abs3.ENTRY);
    }
}
