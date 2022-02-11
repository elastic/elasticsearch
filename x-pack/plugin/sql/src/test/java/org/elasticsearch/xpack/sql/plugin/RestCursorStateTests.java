/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.action.BasicFormatter;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;

import java.util.List;

import static org.elasticsearch.xpack.sql.plugin.RestCursorState.decodeCursorWithState;
import static org.elasticsearch.xpack.sql.plugin.RestCursorState.encodeCursorWithState;
import static org.elasticsearch.xpack.sql.proto.formatter.SimpleFormatter.FormatOption.TEXT;

public class RestCursorStateTests extends ESTestCase {

    public void testWrapAndUnwrapCursorWithFormatter() {
        String cursor = randomAlphaOfLength(100);
        RestCursorState state = new RestCursorState(
            new BasicFormatter(
                List.of(new ColumnInfo(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10))),
                List.of(List.of(randomAlphaOfLength(100))),
                TEXT
            )
        );
        Tuple<String, RestCursorState> result = decodeCursorWithState(encodeCursorWithState(cursor, state));

        assertEquals(cursor, result.v1());
        assertEquals(state, result.v2());
    }

    public void testWrapAndUnwrapCursorWithoutFormatter() {
        String cursor = randomAlphaOfLength(100);

        Tuple<String, RestCursorState> result = decodeCursorWithState(encodeCursorWithState(cursor, RestCursorState.EMPTY));
        assertEquals(cursor, result.v1());
        assertEquals(RestCursorState.EMPTY, result.v2());
    }

    public void testWrapAndUnwrapEmptyCursorWithoutFormatter() {
        Tuple<String, RestCursorState> result = decodeCursorWithState(encodeCursorWithState("", RestCursorState.EMPTY));

        assertEquals("", result.v1());
        assertEquals(RestCursorState.EMPTY, result.v2());
    }

}
