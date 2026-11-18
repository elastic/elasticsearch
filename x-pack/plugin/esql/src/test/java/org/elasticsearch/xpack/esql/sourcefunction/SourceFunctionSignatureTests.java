/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.sourcefunction;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.time.Period;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.sourcefunction.SourceFunctionSignature.namedValues;
import static org.elasticsearch.xpack.esql.sourcefunction.SourceFunctionSignature.optional;
import static org.elasticsearch.xpack.esql.sourcefunction.SourceFunctionSignature.string;
import static org.hamcrest.Matchers.containsString;

/**
 * The shared shape check, exercised directly rather than through a query.
 * <p>
 * Not through the parser, because the parser leaves a call unresolved, as it does a scalar function's: the shape
 * is checked where the call is resolved. Asserting it here keeps the cases and drops the query.
 */
public class SourceFunctionSignatureTests extends ESTestCase {

    private static final SourceFunctionSignature DATASET_LIKE = SourceFunctionSignature.of(
        string("data source"),
        string("resource"),
        optional(namedValues("options"))
    );

    private static SourceFunctionInvocation call(SourceFunctionArgument... arguments) {
        return new SourceFunctionInvocation(Source.EMPTY, "thing", List.of(arguments));
    }

    private static SourceFunctionArgument aString() {
        return new SourceFunctionArgument.Value(Source.EMPTY, DataType.KEYWORD, randomAlphaOfLength(6));
    }

    private static SourceFunctionArgument aConstant(DataType type, Object value) {
        return new SourceFunctionArgument.Value(Source.EMPTY, type, value);
    }

    private static SourceFunctionArgument aMap() {
        return new SourceFunctionArgument.NamedValues(Source.EMPTY, Map.of("k", "v"));
    }

    public void testRequiredArgumentMissing() {
        ParsingException e = expectThrows(ParsingException.class, () -> DATASET_LIKE.check(call(aString())));
        assertThat(e.getMessage(), containsString("[thing] takes [data source, resource, [options]], found [1]"));
    }

    public void testOptionalArgumentMayBeOmitted() {
        DATASET_LIKE.check(call(aString(), aString()));
    }

    public void testOptionalArgumentMayBeGiven() {
        DATASET_LIKE.check(call(aString(), aString(), aMap()));
    }

    public void testTooManyArguments() {
        ParsingException e = expectThrows(ParsingException.class, () -> DATASET_LIKE.check(call(aString(), aString(), aString(), aMap())));
        assertThat(e.getMessage(), containsString("[thing] takes [data source, resource, [options]], found [4]"));
    }

    public void testKindIsCheckedPerPosition() {
        ParsingException e = expectThrows(ParsingException.class, () -> DATASET_LIKE.check(call(aString(), aString(), aString())));
        assertThat(e.getMessage(), containsString("[thing] expects a {...} for [options]"));
    }

    public void testAMapWhereAStringIsExpected() {
        ParsingException e = expectThrows(ParsingException.class, () -> DATASET_LIKE.check(call(aMap(), aString())));
        assertThat(e.getMessage(), containsString("[thing] expects a string for [data source]"));
    }

    /**
     * The grammar admits every constant, so a position declared as a string is the only thing refusing a number,
     * a boolean or an array there — and it refuses them in the one wording every source function shares.
     */
    public void testAConstantThatIsNotAStringWhereAStringIsExpected() {
        for (SourceFunctionArgument argument : List.of(
            aConstant(DataType.INTEGER, 1),
            aConstant(DataType.BOOLEAN, true),
            aConstant(DataType.DOUBLE, 2.5),
            aConstant(DataType.DATE_PERIOD, Period.ofDays(1)),
            aConstant(DataType.KEYWORD, List.of("a", "b"))
        )) {
            ParsingException e = expectThrows(ParsingException.class, () -> DATASET_LIKE.check(call(argument, aString())));
            assertThat(e.getMessage(), containsString("[thing] expects a string for [data source]"));
        }
    }

    /**
     * A written {@code NULL} folds to a constant of type {@code NULL} rather than to a string holding nothing, so it
     * fails the declared kind for the ordinary reason — and it fails it here, against the argument's own name,
     * rather than reaching a function as a name it cannot use.
     */
    public void testAWrittenNullWhereAStringIsExpected() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> DATASET_LIKE.check(call(aConstant(DataType.NULL, null), aString()))
        );
        assertThat(e.getMessage(), containsString("[thing] expects a string for [data source]"));
    }

    public void testNoArgumentsAtAll() {
        SourceFunctionSignature none = SourceFunctionSignature.of();
        none.check(call());
        ParsingException e = expectThrows(ParsingException.class, () -> none.check(call(aString())));
        assertThat(e.getMessage(), containsString("[thing] takes no arguments, found [1]"));
    }

    /**
     * The other declaration guard. A call is positional, so two arguments under one name leave every diagnostic
     * about either of them pointing at a position the reader cannot pick out -- and the names are not otherwise
     * constrained, so nothing else would notice.
     */
    public void testAnArgumentNameCannotBeDeclaredTwice() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> SourceFunctionSignature.of(string("resource"), string("resource"))
        );
        assertThat(e.getMessage(), containsString("argument [resource] is declared twice"));
    }

    /** Across the optional boundary too, where the two guards could have shadowed one another. */
    public void testAnArgumentNameCannotBeDeclaredTwiceAcrossTheOptionalBoundary() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> SourceFunctionSignature.of(string("resource"), optional(namedValues("resource")))
        );
        assertThat(e.getMessage(), containsString("argument [resource] is declared twice"));
    }

    /**
     * The one declaration guard, a shape no call could satisfy. Thrown rather than asserted, so it holds in
     * production, which is the only place a mis-declared function would otherwise surface.
     */
    public void testARequiredArgumentCannotFollowAnOptionalOne() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> SourceFunctionSignature.of(
                SourceFunctionSignature.optional(SourceFunctionSignature.string("first")),
                SourceFunctionSignature.string("second")
            )
        );
        assertThat(e.getMessage(), containsString("required argument [second] follows an optional one"));
    }

}
