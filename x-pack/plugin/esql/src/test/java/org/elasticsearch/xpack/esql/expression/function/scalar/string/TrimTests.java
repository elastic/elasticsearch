/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class TrimTests extends AbstractScalarFunctionTestCase {

    private DataType randomType;

    @Before
    public void setup() {
        randomType = randomFrom(strings());
    }

    @Override
    protected List<Object> simpleData() {
        return List.of(addRandomLeadingOrTrailingWhitespaces(randomUnicodeOfLength(8)));
    }

    @Override
    protected Expression expressionForSimpleData() {
        return new Trim(Source.EMPTY, field(randomUnicodeOfLength(8), randomType));
    }

    @Override
    protected Matcher<Object> resultMatcher(List<Object> data, DataType dataType) {
        return equalTo(new BytesRef(((BytesRef) data.get(0)).utf8ToString().trim()));
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "TrimEvaluator[val=Attribute[channel=0]]";
    }

    @Override
    protected Expression constantFoldable(List<Object> data) {
        return new Trim(Source.EMPTY, new Literal(Source.EMPTY, data.get(0), randomType));
    }

    @Override
    protected Expression build(Source source, List<Literal> args) {
        return new Trim(source, args.get(0));
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(strings()));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return argTypes.get(0);
    }

    public void testTrim() {
        String expected = randomUnicodeOfLength(8).trim();
        BytesRef result = Trim.process(addRandomLeadingOrTrailingWhitespaces(expected));
        assertThat(result.utf8ToString(), equalTo(expected));
    }

    BytesRef addRandomLeadingOrTrailingWhitespaces(String expected) {
        StringBuilder builder = new StringBuilder();
        if (randomBoolean()) {
            builder.append(randomWhiteSpace());
            builder.append(expected);
            if (randomBoolean()) {
                builder.append(randomWhiteSpace());
            }
        } else {
            builder.append(expected);
            builder.append(randomWhiteSpace());
        }
        return new BytesRef(builder.toString());
    }

    private static char[] randomWhiteSpace() {
        char[] randomWhitespace = new char[randomIntBetween(1, 8)];
        Arrays.fill(randomWhitespace, randomFrom(' ', '\t', '\n'));
        return randomWhitespace;
    }

}
