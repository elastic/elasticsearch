/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.tablefunction.TableFunctionSignature.name;
import static org.elasticsearch.xpack.esql.tablefunction.TableFunctionSignature.namedValues;
import static org.elasticsearch.xpack.esql.tablefunction.TableFunctionSignature.optional;
import static org.hamcrest.Matchers.containsString;

/**
 * The shared shape check, exercised directly rather than through a query.
 * <p>
 * These used to be parser tests, which they can no longer be: a call is left unresolved by the parser, as a scalar
 * function's is, so the shape is checked when the call is resolved. Asserting it here keeps the cases and drops the
 * query, which is the cheaper and more direct test of the same statements.
 */
public class TableFunctionSignatureTests extends ESTestCase {

    private static final TableFunctionSignature DATASET_LIKE = TableFunctionSignature.of(
        name("data source"),
        name("resource"),
        optional(namedValues("options"))
    );

    private static TableFunctionInvocation call(TableFunctionArgument... arguments) {
        return new TableFunctionInvocation(Source.EMPTY, "thing", List.of(arguments));
    }

    private static TableFunctionArgument aName() {
        return new TableFunctionArgument.Name(Source.EMPTY, randomAlphaOfLength(6));
    }

    private static TableFunctionArgument aMap() {
        return new TableFunctionArgument.NamedValues(Source.EMPTY, Map.of("k", "v"));
    }

    public void testRequiredArgumentMissing() {
        ParsingException e = expectThrows(ParsingException.class, () -> DATASET_LIKE.check(call(aName())));
        assertThat(e.getMessage(), containsString("[thing] takes [data source, resource, [options]], found [1]"));
    }

    public void testOptionalArgumentMayBeOmitted() {
        DATASET_LIKE.check(call(aName(), aName()));
    }

    public void testOptionalArgumentMayBeGiven() {
        DATASET_LIKE.check(call(aName(), aName(), aMap()));
    }

    public void testTooManyArguments() {
        ParsingException e = expectThrows(ParsingException.class, () -> DATASET_LIKE.check(call(aName(), aName(), aName(), aMap())));
        assertThat(e.getMessage(), containsString("[thing] takes [data source, resource, [options]], found [4]"));
    }

    public void testKindIsCheckedPerPosition() {
        ParsingException e = expectThrows(ParsingException.class, () -> DATASET_LIKE.check(call(aName(), aName(), aName())));
        assertThat(e.getMessage(), containsString("[thing] expects a {...} for [options]"));
    }

    public void testANameWhereAMapIsGiven() {
        ParsingException e = expectThrows(ParsingException.class, () -> DATASET_LIKE.check(call(aMap(), aName())));
        assertThat(e.getMessage(), containsString("[thing] expects a name for [data source]"));
    }

    public void testNoArgumentsAtAll() {
        TableFunctionSignature none = TableFunctionSignature.of();
        none.check(call());
        ParsingException e = expectThrows(ParsingException.class, () -> none.check(call(aName())));
        assertThat(e.getMessage(), containsString("[thing] takes no arguments, found [1]"));
    }

    /**
     * The three declaration guards, each a shape no call could satisfy. Thrown rather than asserted, so these
     * hold in production, which is the only place a mis-declared function would otherwise surface.
     */
    public void testARequiredArgumentCannotFollowAnOptionalOne() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> TableFunctionSignature.of(
                TableFunctionSignature.optional(TableFunctionSignature.name("first")),
                TableFunctionSignature.name("second")
            )
        );
        assertThat(e.getMessage(), containsString("required argument [second] follows an optional one"));
    }

    public void testAMapArgumentMustBeLast() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> TableFunctionSignature.of(TableFunctionSignature.namedValues("options"), TableFunctionSignature.name("after"))
        );
        assertThat(e.getMessage(), containsString("argument [options] takes a {...} and is not last"));
    }

    public void testAMapArgumentCannotBeTheOnlyOne() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> TableFunctionSignature.of(TableFunctionSignature.namedValues("options"))
        );
        assertThat(e.getMessage(), containsString("argument [options] takes a {...} and is the only one"));
    }
}
