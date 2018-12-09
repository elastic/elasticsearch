/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.SqlException;
import org.elasticsearch.xpack.sql.action.CliFormatter;
import org.elasticsearch.xpack.sql.action.SqlQueryResponse;
import org.elasticsearch.xpack.sql.plugin.CliFormatterCursor;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.Cursors;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.action.support.PlainActionFuture.newFuture;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class CursorTests extends ESTestCase {

    public void testEmptyCursorClearCursor() {
        Client clientMock = mock(Client.class);
        Cursor cursor = Cursor.EMPTY;
        PlainActionFuture<Boolean> future = newFuture();
        cursor.clear(Configuration.DEFAULT, clientMock, future);
        assertFalse(future.actionGet());
        verifyZeroInteractions(clientMock);
    }

    @SuppressWarnings("unchecked")
    public void testScrollCursorClearCursor() {
        Client clientMock = mock(Client.class);
        ActionListener<Boolean> listenerMock = mock(ActionListener.class);
        String cursorString = randomAlphaOfLength(10);
        Cursor cursor = new ScrollCursor(cursorString, Collections.emptyList(), randomInt());

        cursor.clear(Configuration.DEFAULT, clientMock, listenerMock);

        ArgumentCaptor<ClearScrollRequest> request = ArgumentCaptor.forClass(ClearScrollRequest.class);
        verify(clientMock).clearScroll(request.capture(), any(ActionListener.class));
        assertEquals(Collections.singletonList(cursorString), request.getValue().getScrollIds());
        verifyZeroInteractions(listenerMock);
    }

    private static SqlQueryResponse createRandomSqlResponse() {
        int columnCount = between(1, 10);

        List<ColumnInfo> columns = null;
        if (randomBoolean()) {
            columns = new ArrayList<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                columns.add(new ColumnInfo(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10),
                        randomInt(), randomInt(25)));
            }
        }
        return new SqlQueryResponse("", columns, Collections.emptyList());
    }

    @SuppressWarnings("unchecked")
    static Cursor randomNonEmptyCursor() {
        Supplier<Cursor> cursorSupplier = randomFrom(
                () -> ScrollCursorTests.randomScrollCursor(),
                () -> {
                    SqlQueryResponse response = createRandomSqlResponse();
                    if (response.columns() != null && response.rows() != null) {
                        return CliFormatterCursor.wrap(ScrollCursorTests.randomScrollCursor(),
                            new CliFormatter(response.columns(), response.rows()));
                    } else {
                        return ScrollCursorTests.randomScrollCursor();
                    }

                }
        );
        return cursorSupplier.get();
    }

    public void testVersionHandling() {
        Cursor cursor = randomNonEmptyCursor();
        assertEquals(cursor, Cursors.decodeFromString(Cursors.encodeToString(Version.CURRENT, cursor)));

        Version nextMinorVersion = Version.fromId(Version.CURRENT.id + 10000);

        String encodedWithWrongVersion = Cursors.encodeToString(nextMinorVersion, cursor);
        SqlException exception = expectThrows(SqlException.class, () -> Cursors.decodeFromString(encodedWithWrongVersion));

        assertEquals("Unsupported cursor version " + nextMinorVersion, exception.getMessage());
    }


}
