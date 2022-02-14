/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.action.BasicFormatter;
import org.elasticsearch.xpack.sql.execution.search.ScrollCursor;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.proto.StringUtils;
import org.elasticsearch.xpack.sql.proto.formatter.SimpleFormatter;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.Cursors;
import org.mockito.ArgumentCaptor;

import java.time.ZoneId;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.action.support.PlainActionFuture.newFuture;
import static org.elasticsearch.xpack.sql.execution.search.ScrollCursorTests.randomScrollCursor;
import static org.elasticsearch.xpack.sql.session.Cursors.attachState;
import static org.elasticsearch.xpack.sql.session.Cursors.decodeFromStringWithZone;
import static org.elasticsearch.xpack.sql.session.Cursors.encodeToString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class CursorTests extends ESTestCase {

    public void testEmptyCursorClearCursor() {
        Client clientMock = mock(Client.class);
        Cursor cursor = Cursor.EMPTY;
        PlainActionFuture<Boolean> future = newFuture();
        cursor.clear(clientMock, future);
        assertFalse(future.actionGet());
        verifyNoMoreInteractions(clientMock);
    }

    @SuppressWarnings("unchecked")
    public void testScrollCursorClearCursor() {
        Client clientMock = mock(Client.class);
        ActionListener<Boolean> listenerMock = mock(ActionListener.class);
        String cursorString = randomAlphaOfLength(10);
        Cursor cursor = new ScrollCursor(cursorString, Collections.emptyList(), new BitSet(0), randomInt());

        cursor.clear(clientMock, listenerMock);

        ArgumentCaptor<ClearScrollRequest> request = ArgumentCaptor.forClass(ClearScrollRequest.class);
        verify(clientMock).clearScroll(request.capture(), any(ActionListener.class));
        assertEquals(Collections.singletonList(cursorString), request.getValue().getScrollIds());
        verifyNoMoreInteractions(listenerMock);
    }

    public void testVersionHandling() {
        Cursor cursor = randomScrollCursor();
        assertEquals(cursor, decodeFromString(encodeToString(cursor, randomZone())));

        Version nextMinorVersion = Version.fromId(Version.CURRENT.id + 10000);

        String encodedWithWrongVersion = encodeToString(cursor, nextMinorVersion, randomZone());
        SqlIllegalArgumentException exception = expectThrows(
            SqlIllegalArgumentException.class,
            () -> decodeFromString(encodedWithWrongVersion)
        );

        assertEquals(
            LoggerMessageFormat.format("Unsupported cursor version [{}], expected [{}]", nextMinorVersion, Version.CURRENT),
            exception.getMessage()
        );
    }

    private static final NamedWriteableRegistry WRITEABLE_REGISTRY = new NamedWriteableRegistry(Cursors.getNamedWriteables());

    public static Cursor decodeFromString(String base64) {
        return decodeFromStringWithZone(base64, WRITEABLE_REGISTRY).v1();
    }

    public void testAttachingStateToCursor() {
        Cursor cursor = randomScrollCursor();
        ZoneId zone = randomZone();
        String encoded = encodeToString(cursor, zone);

        FormatterState state = randomFormatterState();
        String withState = attachState(encoded, state);

        Tuple<Cursor, ZoneId> decoded = decodeFromStringWithZone(withState, WRITEABLE_REGISTRY);
        assertEquals(cursor, decoded.v1());
        assertEquals(zone, decoded.v2());
        assertEquals(state, Cursors.decodeState(withState));
    }

    public void testAttachingEmptyStateToCursor() {
        Cursor cursor = randomScrollCursor();
        ZoneId zone = randomZone();
        String encoded = encodeToString(cursor, zone);

        String withState = attachState(encoded, FormatterState.EMPTY);

        Tuple<Cursor, ZoneId> decoded = decodeFromStringWithZone(withState, WRITEABLE_REGISTRY);
        assertEquals(cursor, decoded.v1());
        assertEquals(zone, decoded.v2());
        assertEquals(FormatterState.EMPTY, Cursors.decodeState(withState));
    }

    public void testAttachingStateToEmptyCursor() {
        Cursor cursor = Cursor.EMPTY;
        ZoneId zone = randomZone();
        String encoded = encodeToString(cursor, zone);

        FormatterState state = randomFormatterState();
        String withState = attachState(encoded, state);

        assertEquals(StringUtils.EMPTY, withState);

        Tuple<Cursor, ZoneId> decoded = decodeFromStringWithZone(withState, WRITEABLE_REGISTRY);
        assertEquals(cursor, decoded.v1());
        assertNull(decoded.v2());
        assertEquals(FormatterState.EMPTY, Cursors.decodeState(withState));
    }

    public void testAttachingStateToCursorFromOtherVersion() {
        Cursor cursor = randomScrollCursor();
        ZoneId zone = randomZone();
        String encoded = encodeToString(cursor, Version.V_7_17_1, zone);

        FormatterState state = randomFormatterState();
        String withState = attachState(encoded, state);

        assertEquals(state, Cursors.decodeState(withState));
        expectThrows(SqlIllegalArgumentException.class, () -> Cursors.decodeFromStringWithZone(withState, WRITEABLE_REGISTRY));
    }

    private FormatterState randomFormatterState() {
        int cols = randomInt(3);
        return new FormatterState(
            new BasicFormatter(
                randomList(cols, cols, () -> new ColumnInfo(randomAlphaOfLength(5), randomAlphaOfLength(5), randomAlphaOfLength(5))),
                List.of(randomList(cols, cols, () -> List.of(randomAlphaOfLength(5)))),
                randomBoolean() ? SimpleFormatter.FormatOption.TEXT : SimpleFormatter.FormatOption.CLI
            )
        );
    }
}
