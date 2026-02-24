/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.testfunction;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.plugin.EsqlFunctionProvider;

import java.util.Collection;

/**
 * Integration test that verifies the plugin discovery mechanism works correctly.
 * <p>
 * This test verifies that:
 * <ul>
 *   <li>TestFunctionPlugin implements EsqlFunctionProvider</li>
 *   <li>The plugin provides the abs3 function definition</li>
 *   <li>The function can be registered in EsqlFunctionRegistry</li>
 *   <li>The registered function is accessible by name</li>
 * </ul>
 */
public class PluginIntegrationTests extends ESTestCase {

    /**
     * Test that TestFunctionPlugin provides the abs3 function.
     */
    public void testPluginProvidesAbs2Function() {
        TestFunctionPlugin plugin = new TestFunctionPlugin();
        assertTrue("Plugin should implement EsqlFunctionProvider", plugin instanceof EsqlFunctionProvider);

        Collection<FunctionDefinition> functions = plugin.getEsqlFunctions();
        assertNotNull("Plugin should provide functions", functions);
        assertEquals("Plugin should provide exactly one function", 1, functions.size());

        FunctionDefinition abs3Def = functions.iterator().next();
        assertEquals("Function name should be abs3", "abs3", abs3Def.name());
    }

    /**
     * Test that the abs3 function can be registered and accessed in EsqlFunctionRegistry.
     */
    public void testAbs2FunctionCanBeRegistered() {
        TestFunctionPlugin plugin = new TestFunctionPlugin();
        Collection<FunctionDefinition> functions = plugin.getEsqlFunctions();

        // Create a registry and register the external functions
        EsqlFunctionRegistry registry = new EsqlFunctionRegistry();
        registry.registerExternalFunctions(functions);

        // Verify the function is registered
        assertTrue("abs3 function should be registered", registry.functionExists("abs3"));

        // Verify we can resolve the function
        FunctionDefinition resolvedDef = registry.resolveFunction("abs3");
        assertNotNull("Should be able to resolve abs3 function", resolvedDef);
        assertEquals("Resolved function name should be abs3", "abs3", resolvedDef.name());
    }

    /**
     * Test that the plugin discovery mechanism would work end-to-end.
     * This simulates what happens in EsqlPlugin.loadExtensions() and PlanExecutor constructor.
     */
    public void testEndToEndPluginDiscovery() {
        // Simulate plugin discovery (what ExtensiblePlugin.loadExtensions does)
        TestFunctionPlugin functionPlugin = new TestFunctionPlugin();
        Collection<FunctionDefinition> externalFunctions = functionPlugin.getEsqlFunctions();

        // Simulate function registry creation (what PlanExecutor does)
        EsqlFunctionRegistry registry = new EsqlFunctionRegistry();
        registry.registerExternalFunctions(externalFunctions);

        // Verify the function is available
        assertTrue("abs3 should be available in registry", registry.functionExists("abs3"));

        // Verify we can get the function definition
        FunctionDefinition abs3 = registry.resolveFunction("abs3");
        assertEquals("abs3", abs3.name());
        assertEquals(Abs3.class, abs3.clazz());
    }
}
