/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.tree.Location;

import java.util.Arrays;
import java.util.Collections;

import static org.elasticsearch.xpack.sql.tree.Location.EMPTY;

public class IFNullTests extends ESTestCase {

    public void testIfNullInvalidNumberOfArgs() {
        SqlIllegalArgumentException ex = expectThrows(SqlIllegalArgumentException.class, () ->
            new IFNull(new Location(1, 1), Collections.singletonList(Literal.of(EMPTY, "foo"))).makePipe());
        assertEquals("Line 1:2: Unexpected number of arguments: expected [2], received[1]", ex.getMessage());

        ex = expectThrows(SqlIllegalArgumentException.class, () ->
            new IFNull(new Location(1, 1),
                Arrays.asList(Literal.of(EMPTY, "foo"), Literal.NULL, Literal.of(EMPTY, "bar"))).makePipe());
        assertEquals("Line 1:2: Unexpected number of arguments: expected [2], received[3]", ex.getMessage());
    }
}
