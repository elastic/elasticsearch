/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

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
        public List<String> dataSourceNames(TableFunctionInvocation call) {
            throw new AssertionError("the registry must not read a call's data sources");
        }

        @Override
        public LogicalPlan toRelation(TableFunctionInvocation call, List<NamedExpression> metadataFields, TableFunctionContext context) {
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

    public void testDatasetIsRegistered() {
        assertEquals(DatasetTableFunction.NAME, TableFunctionRegistry.of(Map.of()).resolve(Source.EMPTY, "DATASET").name());
    }
}
