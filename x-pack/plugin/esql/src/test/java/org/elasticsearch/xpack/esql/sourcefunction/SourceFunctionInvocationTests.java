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

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * What an invocation says about itself when something logs one. A record's generated {@code toString} would render
 * every argument, and an argument can be a definition carrying a credential.
 */
public class SourceFunctionInvocationTests extends ESTestCase {

    private static final String SECRET = "AKIAIOSFODNN7EXAMPLE";

    private static SourceFunctionInvocation callCarryingASecret() {
        return new SourceFunctionInvocation(
            Source.EMPTY,
            "somefunc",
            List.of(
                new SourceFunctionArgument.Value(Source.EMPTY, DataType.KEYWORD, "a_name"),
                new SourceFunctionArgument.NamedValues(Source.EMPTY, Map.of("access_key", SECRET))
            )
        );
    }

    public void testToStringCarriesTheNameAndACountRatherThanTheArguments() {
        SourceFunctionInvocation call = callCarryingASecret();

        assertThat(call.toString(), equalTo("SourceFunctionInvocation[name=somefunc, arguments=2]"));
        assertThat(call.toString(), not(containsString(SECRET)));
        assertThat(call.toString(), not(containsString("a_name")));
    }
}
