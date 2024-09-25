/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.asLongUnsigned;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

abstract class AbstractStatementParserTests extends ESTestCase {

    EsqlParser parser = new EsqlParser();

    void assertStatement(String statement, LogicalPlan expected) {
        final LogicalPlan actual;
        try {
            actual = statement(statement);
        } catch (Exception e) {
            throw new AssertionError("parsing error for [" + statement + "]", e);
        }
        assertThat(statement, actual, equalTo(expected));
    }

    LogicalPlan statement(String e) {
        return statement(e, new QueryParams());
    }

    LogicalPlan statement(String e, QueryParams params) {
        return parser.createStatement(e, params);
    }

    LogicalPlan processingCommand(String e) {
        return parser.createStatement("row a = 1 | " + e);
    }

    static UnresolvedAttribute attribute(String name) {
        return new UnresolvedAttribute(EMPTY, name);
    }

    static Literal integer(int i) {
        return new Literal(EMPTY, i, DataType.INTEGER);
    }

    static Literal integers(int... ints) {
        return new Literal(EMPTY, Arrays.stream(ints).boxed().toList(), DataType.INTEGER);
    }

    static Literal literalLong(long i) {
        return new Literal(EMPTY, i, DataType.LONG);
    }

    static Literal literalLongs(long... longs) {
        return new Literal(EMPTY, Arrays.stream(longs).boxed().toList(), DataType.LONG);
    }

    static Literal literalDouble(double d) {
        return new Literal(EMPTY, d, DataType.DOUBLE);
    }

    static Literal literalDoubles(double... doubles) {
        return new Literal(EMPTY, Arrays.stream(doubles).boxed().toList(), DataType.DOUBLE);
    }

    static Literal literalUnsignedLong(String ulong) {
        return new Literal(EMPTY, asLongUnsigned(new BigInteger(ulong)), DataType.UNSIGNED_LONG);
    }

    static Literal literalUnsignedLongs(String... ulongs) {
        return new Literal(EMPTY, Arrays.stream(ulongs).map(s -> asLongUnsigned(new BigInteger(s))).toList(), DataType.UNSIGNED_LONG);
    }

    static Literal literalBoolean(boolean b) {
        return new Literal(EMPTY, b, DataType.BOOLEAN);
    }

    static Literal literalBooleans(boolean... booleans) {
        List<Boolean> v = new ArrayList<>(booleans.length);
        for (boolean b : booleans) {
            v.add(b);
        }
        return new Literal(EMPTY, v, DataType.BOOLEAN);
    }

    static Literal literalString(String s) {
        return new Literal(EMPTY, s, DataType.KEYWORD);
    }

    static Literal literalStrings(String... strings) {
        return new Literal(EMPTY, Arrays.asList(strings), DataType.KEYWORD);
    }

    void expectError(String query, String errorMessage) {
        ParsingException e = expectThrows(ParsingException.class, "Expected syntax error for " + query, () -> statement(query));
        assertThat(e.getMessage(), containsString(errorMessage));
    }

    void expectVerificationError(String query, String errorMessage) {
        VerificationException e = expectThrows(VerificationException.class, "Expected syntax error for " + query, () -> statement(query));
        assertThat(e.getMessage(), containsString(errorMessage));
    }

    void expectError(String query, List<QueryParam> params, String errorMessage) {
        ParsingException e = expectThrows(
            ParsingException.class,
            "Expected syntax error for " + query,
            () -> statement(query, new QueryParams(params))
        );
        assertThat(e.getMessage(), containsString(errorMessage));
    }
}
