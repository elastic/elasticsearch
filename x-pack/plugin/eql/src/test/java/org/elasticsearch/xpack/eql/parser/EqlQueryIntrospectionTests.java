/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.parser;

import org.elasticsearch.test.ESTestCase;

/**
 * Direct tests for {@link EqlQueryIntrospection}: the eql-module facade that reports an EQL query's result mode and
 * whether it carries its own explicit {@code head}/{@code tail} limit, both derived by parsing the query string. The
 * ES|QL {@code EQL} source command relies on both to fix its schema and to decide whether a row {@code LIMIT} is safe
 * to push into the request size.
 */
public class EqlQueryIntrospectionTests extends ESTestCase {

    public void testModeEvent() {
        assertEquals(EqlQueryIntrospection.Mode.EVENT, EqlQueryIntrospection.mode("process where true"));
    }

    public void testModeSequence() {
        assertEquals(EqlQueryIntrospection.Mode.SEQUENCE, EqlQueryIntrospection.mode("sequence [process where true] [network where true]"));
    }

    public void testModeSample() {
        assertEquals(
            EqlQueryIntrospection.Mode.SAMPLE,
            EqlQueryIntrospection.mode("sample by category [process where true] [network where true]")
        );
    }

    public void testNoExplicitLimit() {
        // The parser inserts exactly one implicit head/tail limit, so a query with no user-written pipe has just one.
        assertFalse(EqlQueryIntrospection.hasExplicitLimit("process where true"));
        assertFalse(EqlQueryIntrospection.hasExplicitLimit("sequence [process where true] [network where true]"));
        assertFalse(EqlQueryIntrospection.hasExplicitLimit("sample by category [process where true] [network where true]"));
    }

    public void testExplicitHeadOrTailLimit() {
        // A user-written | head/| tail adds a second limit node on top of the implicit one.
        assertTrue(EqlQueryIntrospection.hasExplicitLimit("process where true | head 5"));
        assertTrue(EqlQueryIntrospection.hasExplicitLimit("process where true | tail 3"));
        assertTrue(EqlQueryIntrospection.hasExplicitLimit("sequence [process where true] [network where true] | head 2"));
    }

    public void testMalformedQueryThrows() {
        // Introspection parses the query, so a malformed one throws (the ES|QL command turns this into a ParsingException).
        expectThrows(Exception.class, () -> EqlQueryIntrospection.mode("this is not eql"));
    }
}
