/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.functions.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
 * This plugin registers the test functions which use runtime bytecode generation
 * instead of compile-time code generation:
 * </p>
 * <ul>
 *   <li>{@link Abs2} - unary function</li>
 *   <li>{@link Add2} - binary function</li>
 *   <li>{@link Upper2} - string function</li>
 *   <li>{@link Pi2} - zero-parameter function</li>
 *   <li>{@link Left2} - function with @RuntimeFixed parameter</li>
 *   <li>{@link Div2} - function with warnExceptions</li>
 *   <li>{@link Greatest2} - variadic function</li>
 *   <li>{@link Concat2} - variadic string concatenation function</li>
 *   <li>{@link MvSum2} - MvEvaluator function (multi-value reducer)</li>
 *   <li>{@link MvMax2} - MvEvaluator with ascending optimization</li>
 *   <li>{@link MvAvg2} - MvEvaluator with single value optimization</li>
 *   <li>{@link Sum2} - aggregate function using runtime aggregator generation</li>
 *   <li>{@link LengthSum2} - aggregate function with BytesRef input using runtime aggregator generation</li>
 *   <li>{@link SafeSum2} - aggregate function with warnExceptions for overflow protection</li>
 *   <li>{@link DoubleSum2} - aggregate function with double state type</li>
 *   <li>{@link IntCount2} - aggregate function with int state type</li>
 *   <li>{@link MaxBytes2} - aggregate function with BytesRef state and first method</li>
 * </ul>
 * <p>
 * The plugin implements {@link EsqlFunctionProvider} to provide functions to the
 * ES|QL engine. It extends the x-pack-esql plugin (declared in build.gradle).
 * </p>
 */
public class TestFunctionsPlugin extends Plugin implements EsqlFunctionProvider {
    private static final Logger logger = LogManager.getLogger(TestFunctionsPlugin.class);

    public TestFunctionsPlugin() {
        logger.info("TestFunctionsPlugin loaded - providing test functions");
    }

    @Override
    public Collection<FunctionDefinition> getEsqlFunctions() {
        logger.info("getEsqlFunctions() called - registering test functions");

        // Explicitly cast Pi2::new to Function<Source, Pi2> to ensure correct overload resolution
        java.util.function.Function<org.elasticsearch.xpack.esql.core.tree.Source, Pi2> pi2Ctor = Pi2::new;

        FunctionDefinition abs2Def = EsqlFunctionRegistry.createRuntimeDef(Abs2.class, Abs2::new, "abs2");
        FunctionDefinition add2Def = EsqlFunctionRegistry.createRuntimeDef(Add2.class, Add2::new, "add2");
        FunctionDefinition upper2Def = EsqlFunctionRegistry.createRuntimeDef(Upper2.class, Upper2::new, "upper2");
        FunctionDefinition pi2Def = EsqlFunctionRegistry.createRuntimeDef(Pi2.class, pi2Ctor, "pi2");
        FunctionDefinition left2Def = EsqlFunctionRegistry.createRuntimeDef(Left2.class, Left2::new, "left2");
        FunctionDefinition div2Def = EsqlFunctionRegistry.createRuntimeDef(Div2.class, Div2::new, "div2");
        FunctionDefinition greatest2Def = EsqlFunctionRegistry.createRuntimeDef(Greatest2.class, Greatest2::new, "greatest2");
        FunctionDefinition concat2Def = EsqlFunctionRegistry.createRuntimeDef(Concat2.class, Concat2::new, "concat2");
        FunctionDefinition mvSum2Def = EsqlFunctionRegistry.createRuntimeDef(MvSum2.class, MvSum2::new, "mv_sum2");
        FunctionDefinition mvMax2Def = EsqlFunctionRegistry.createRuntimeDef(MvMax2.class, MvMax2::new, "mv_max2");
        FunctionDefinition mvAvg2Def = EsqlFunctionRegistry.createRuntimeDef(MvAvg2.class, MvAvg2::new, "mv_avg2");
        FunctionDefinition sum2Def = EsqlFunctionRegistry.createRuntimeDef(Sum2.class, Sum2::new, "sum2");
        FunctionDefinition lengthSum2Def = EsqlFunctionRegistry.createRuntimeDef(LengthSum2.class, LengthSum2::new, "length_sum2");
        FunctionDefinition safeSum2Def = EsqlFunctionRegistry.createRuntimeDef(SafeSum2.class, SafeSum2::new, "safe_sum2");
        FunctionDefinition doubleSum2Def = EsqlFunctionRegistry.createRuntimeDef(DoubleSum2.class, DoubleSum2::new, "double_sum2");
        FunctionDefinition intCount2Def = EsqlFunctionRegistry.createRuntimeDef(IntCount2.class, IntCount2::new, "int_count2");
        FunctionDefinition maxBytes2Def = EsqlFunctionRegistry.createRuntimeDef(MaxBytes2.class, MaxBytes2::new, "max_bytes2");

        List<FunctionDefinition> functions = List.of(
            abs2Def,
            add2Def,
            upper2Def,
            pi2Def,
            left2Def,
            div2Def,
            greatest2Def,
            concat2Def,
            mvSum2Def,
            mvMax2Def,
            mvAvg2Def,
            sum2Def,
            lengthSum2Def,
            safeSum2Def,
            doubleSum2Def,
            intCount2Def,
            maxBytes2Def
        );
        logger.info("Returning {} function definitions", functions.size());

        return functions;
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            Abs2.ENTRY,
            Add2.ENTRY,
            Upper2.ENTRY,
            Pi2.ENTRY,
            Left2.ENTRY,
            Div2.ENTRY,
            Greatest2.ENTRY,
            Concat2.ENTRY,
            MvSum2.ENTRY,
            MvMax2.ENTRY,
            MvAvg2.ENTRY,
            Sum2.ENTRY,
            LengthSum2.ENTRY,
            SafeSum2.ENTRY,
            DoubleSum2.ENTRY,
            IntCount2.ENTRY,
            MaxBytes2.ENTRY
        );
    }
}
