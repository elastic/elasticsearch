/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.proto.StringUtils;
import org.elasticsearch.xpack.sql.proto.formatter.SimpleFormatter;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.Cursors;

import java.time.ZoneId;
import java.util.List;

import static org.elasticsearch.xpack.sql.execution.search.SearchHitCursorTests.randomSearchHitCursor;
import static org.elasticsearch.xpack.sql.session.Cursors.attachFormatter;
import static org.elasticsearch.xpack.sql.session.Cursors.decodeFromStringWithZone;
import static org.elasticsearch.xpack.sql.session.Cursors.encodeToString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class CursorTests extends ESTestCase {

    public void testEmptyCursorClearCursor() {
        Client clientMock = mock(Client.class);
        Cursor cursor = Cursor.EMPTY;
        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        cursor.clear(clientMock, future);
        assertFalse(future.actionGet());
        verifyNoMoreInteractions(clientMock);
    }

    private static final NamedWriteableRegistry WRITEABLE_REGISTRY = new NamedWriteableRegistry(Cursors.getNamedWriteables());

    public static Cursor decodeFromString(String base64) {
        return decodeFromStringWithZone(base64, WRITEABLE_REGISTRY).v1();
    }

    public void testAttachingFormatterToCursor() {
        Cursor cursor = randomSearchHitCursor();
        ZoneId zone = randomZone();
        String encoded = encodeToString(cursor, zone);

        BasicFormatter formatter = randomFormatter();
        String withFormatter = attachFormatter(encoded, formatter);

        Tuple<Cursor, ZoneId> decoded = decodeFromStringWithZone(withFormatter, WRITEABLE_REGISTRY);
        assertEquals(cursor, decoded.v1());
        assertEquals(zone, decoded.v2());
        assertEquals(formatter, Cursors.decodeFormatter(withFormatter));
    }

    public void testAttachingEmptyFormatterToCursor() {
        Cursor cursor = randomSearchHitCursor();
        ZoneId zone = randomZone();
        String encoded = encodeToString(cursor, zone);

        String withFormatter = attachFormatter(encoded, null);

        Tuple<Cursor, ZoneId> decoded = decodeFromStringWithZone(withFormatter, WRITEABLE_REGISTRY);
        assertEquals(cursor, decoded.v1());
        assertEquals(zone, decoded.v2());
        assertNull(Cursors.decodeFormatter(withFormatter));
    }

    public void testAttachingFormatterToEmptyCursor() {
        Cursor cursor = Cursor.EMPTY;
        ZoneId zone = randomZone();
        String encoded = encodeToString(cursor, zone);

        BasicFormatter formatter = randomFormatter();
        String withFormatter = attachFormatter(encoded, formatter);

        assertEquals(StringUtils.EMPTY, withFormatter);

        Tuple<Cursor, ZoneId> decoded = decodeFromStringWithZone(withFormatter, WRITEABLE_REGISTRY);
        assertEquals(cursor, decoded.v1());
        assertNull(decoded.v2());
        assertNull(Cursors.decodeFormatter(withFormatter));
    }

    private BasicFormatter randomFormatter() {
        int cols = randomInt(3);
        return new BasicFormatter(
            randomList(cols, cols, () -> new ColumnInfo(randomAlphaOfLength(5), randomAlphaOfLength(5), randomAlphaOfLength(5))),
            List.of(randomList(cols, cols, () -> List.of(randomAlphaOfLength(5)))),
            randomBoolean() ? SimpleFormatter.FormatOption.TEXT : SimpleFormatter.FormatOption.CLI
        );
    }
}
