/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

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
            ProjectMetadata projectMetadata,
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

    /**
     * A name no query can lex is refused where it is registered. Nothing downstream would refuse it: the token is
     * {@code LETTER UNQUOTED_ID_BODY* '('}, so each of these lexes as an index pattern followed by a stray
     * {@code (} and the function would sit in the registry unreachable rather than fail anywhere a reader looks.
     */
    public void testNameMustBeOneAQueryCanLex() {
        for (String name : List.of("_thing", "1thing", "ex-ample", "ex.ample", "ex ample")) {
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> new TableFunctionRegistry(List.of(new NamedOnly(name)))
            );
            assertThat(e.getMessage(), containsString("table function name [" + name + "] must start with a letter"));
        }
    }

    /** And the shapes that do lex are accepted, so the check is not refusing everything. */
    public void testALexableNameIsAccepted() {
        for (String name : List.of("thing", "thing2", "thing_two", "t")) {
            assertThat(new TableFunctionRegistry(List.of(new NamedOnly(name))).resolve(Source.EMPTY, name).name(), equalTo(name));
        }
    }

    public void testRegistryStartsEmpty() {
        // An empty registry answers without listing an empty set, which would read as an engine fault rather
        // than as an answer about the name that was asked for.
        ParsingException e = expectThrows(ParsingException.class, () -> new TableFunctionRegistry(List.of()).resolve(Source.EMPTY, "x"));
        assertThat(e.getMessage(), containsString("no table functions are registered, so [x] cannot be used here"));
    }

}
