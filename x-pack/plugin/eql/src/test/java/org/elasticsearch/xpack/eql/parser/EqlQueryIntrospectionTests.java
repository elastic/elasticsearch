/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.parser;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.eql.parser.EqlQueryIntrospection.Mode.EVENT;
import static org.elasticsearch.xpack.eql.parser.EqlQueryIntrospection.Mode.SAMPLE;
import static org.elasticsearch.xpack.eql.parser.EqlQueryIntrospection.Mode.SEQUENCE;
import static org.elasticsearch.xpack.eql.parser.EqlQueryIntrospection.inspect;

/**
 * Direct tests for {@link EqlQueryIntrospection}: the eql-module facade that reports an EQL query's result mode and
 * whether it carries its own explicit {@code head}/{@code tail} limit, both derived by parsing the query string. The
 * ES|QL {@code EQL} source command relies on both to fix its schema and to decide whether a row {@code LIMIT} is safe
 * to push into the request size.
 */
public class EqlQueryIntrospectionTests extends ESTestCase {

    public void testEventQuery() {
        assertEquals(new EqlQueryIntrospection.Shape(EVENT, false), inspect("process where true"));
    }

    public void testSequenceQuery() {
        assertEquals(new EqlQueryIntrospection.Shape(SEQUENCE, false), inspect("sequence [process where true] [network where true]"));
    }

    public void testSampleQuery() {
        assertEquals(
            new EqlQueryIntrospection.Shape(SAMPLE, false),
            inspect("sample by category [process where true] [network where true]")
        );
    }

    public void testExplicitHeadOrTailLimit() {
        assertEquals(new EqlQueryIntrospection.Shape(EVENT, true), inspect("process where true | head 5"));
        assertEquals(new EqlQueryIntrospection.Shape(EVENT, true), inspect("process where true | tail 3"));
        assertEquals(
            new EqlQueryIntrospection.Shape(SEQUENCE, true),
            inspect("sequence [process where true] [network where true] | head 2")
        );
        assertEquals(new EqlQueryIntrospection.Shape(EVENT, true), inspect("process where true | head 5 | tail 2"));
    }

    public void testMalformedQueryThrows() {
        expectThrows(ParsingException.class, () -> inspect("this is not eql"));
    }
}
