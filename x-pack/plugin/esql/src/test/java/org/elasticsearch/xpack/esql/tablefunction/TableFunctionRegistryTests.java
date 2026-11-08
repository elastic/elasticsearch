/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class TableFunctionRegistryTests extends ESTestCase {

    /** A function that is only a name: the registry resolves one, it never calls through to it. */
    private record NamedOnly(String name) implements TableFunction {
        @Override
        public TableFunctionSignature signature() {
            throw new AssertionError("the registry must not read a call's shape");
        }

        @Override
        public void validate(TableFunctionInvocation call) {
            throw new AssertionError("the registry must not validate a call");
        }

        @Override
        public void toRelation(
            TableFunctionInvocation call,
            List<NamedExpression> metadataFields,
            TableFunctionContext context,
            ActionListener<LeafPlan> listener
        ) {
            throw new AssertionError("the registry must not bind a call");
        }
    }

    public void testResolveIgnoresCase() {
        TableFunction function = new NamedOnly("thing");
        TableFunctionRegistry registry = new TableFunctionRegistry(List.of(function));
        for (String written : List.of("thing", "THING", "Thing", "tHiNg")) {
            assertSame(written, function, registry.resolve(Source.EMPTY, written));
        }
    }

    public void testUnknownNameListsTheRegisteredOnes() {
        TableFunctionRegistry registry = new TableFunctionRegistry(List.of(new NamedOnly("beta"), new NamedOnly("alpha")));
        ParsingException e = expectThrows(ParsingException.class, () -> registry.resolve(Source.EMPTY, "gamma"));
        assertThat(e.getMessage(), containsString("unknown table function [gamma], expected one of [alpha, beta]"));
    }

    public void testDuplicateNameIsRejected() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new TableFunctionRegistry(List.of(new NamedOnly("thing"), new NamedOnly("thing")))
        );
        assertThat(e.getMessage(), containsString("duplicate table function [thing]"));
    }

    public void testNameMustBeLowercase() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new TableFunctionRegistry(List.of(new NamedOnly("Thing")))
        );
        assertThat(e.getMessage(), containsString("table function name [Thing] must be lowercase"));
    }

    public void testRegistryStartsEmpty() {
        // An empty registry answers without listing an empty set, which would read as an engine fault rather
        // than as an answer about the name that was asked for.
        ParsingException e = expectThrows(ParsingException.class, () -> new TableFunctionRegistry(List.of()).resolve(Source.EMPTY, "x"));
        assertThat(e.getMessage(), containsString("no table functions are registered, so [x] cannot be used here"));
    }

    /**
     * One capability per registered function, so a client can ask for a particular one rather than only asking
     * whether the facility exists.
     * <p>
     * Asked for every capability rather than the enabled ones, because the facility's own capability is gated on a
     * snapshot build: with the enabled set, this asserted nothing on a release build and failed there instead,
     * which the pull request's own CI cannot see and the periodic release-tests job can. What is being checked is
     * that the registry contributes a name per function, and an empty registry still contributes none below.
     */
    public void testCapabilityPerRegisteredFunction() {
        TableFunctionRegistry registry = new TableFunctionRegistry(List.of(new NamedOnly("thing"), new NamedOnly("other")));

        Set<String> capabilities = EsqlCapabilities.capabilities(new EsqlFunctionRegistry(), registry, true).capabilities();

        assertThat(capabilities, hasItem("table_fn_thing"));
        assertThat(capabilities, hasItem("table_fn_other"));
        // The facility's own capability is a separate question from whether a given function exists.
        assertThat(capabilities, hasItem("from_table_function"));
    }

    /** An empty registry contributes no per-function capability. Every capability again, for the same reason. */
    public void testNoCapabilitiesWithoutFunctions() {
        Set<String> capabilities = EsqlCapabilities.capabilities(new EsqlFunctionRegistry(), new TableFunctionRegistry(List.of()), true)
            .capabilities();

        assertThat(capabilities.stream().anyMatch(c -> c.startsWith("table_fn_")), equalTo(false));
    }
}
