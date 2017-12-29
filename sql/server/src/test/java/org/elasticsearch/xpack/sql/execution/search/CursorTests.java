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
import org.elasticsearch.xpack.sql.plugin.CliFormatter;
import org.elasticsearch.xpack.sql.plugin.CliFormatterCursor;
import org.elasticsearch.xpack.sql.plugin.JdbcCursor;
import org.elasticsearch.xpack.sql.plugin.SqlResponse;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.mockito.ArgumentCaptor;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.action.support.PlainActionFuture.newFuture;
import static org.hamcrest.Matchers.hasSize;
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

    private static SqlResponse createRandomSqlResponse() {
        int columnCount = between(1, 10);

        List<SqlResponse.ColumnInfo> columns = null;
        if (randomBoolean()) {
            columns = new ArrayList<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                columns.add(new SqlResponse.ColumnInfo(randomAlphaOfLength(10), randomAlphaOfLength(10),
                    randomFrom(JDBCType.values()), randomInt(25)));
            }
        }
        return new SqlResponse("", columns, Collections.emptyList());
    }

    static Cursor randomNonEmptyCursor() {
        switch (randomIntBetween(0, 2)) {
            case 0:
                return ScrollCursorTests.randomScrollCursor();
            case 1:
                int typeNum = randomIntBetween(0, 10);
                List<JDBCType> types = new ArrayList<>();
                for (int i = 0; i < typeNum; i++) {
                    types.add(randomFrom(JDBCType.values()));
                }
                return JdbcCursor.wrap(ScrollCursorTests.randomScrollCursor(), types);
            case 2:
                SqlResponse response = createRandomSqlResponse();
                if (response.columns() != null && response.rows() != null) {
                    return CliFormatterCursor.wrap(ScrollCursorTests.randomScrollCursor(), new CliFormatter(response));
                } else {
                    return ScrollCursorTests.randomScrollCursor();
                }
            default:
                throw new IllegalArgumentException("Unexpected random value ");
        }
    }

    public void testVersionHandling() {
        Cursor cursor = randomNonEmptyCursor();
        assertEquals(cursor, Cursor.decodeFromString(Cursor.encodeToString(Version.CURRENT, cursor)));

        Version nextMinorVersion = Version.fromId(Version.CURRENT.id + 10000);

        String encodedWithWrongVersion = Cursor.encodeToString(nextMinorVersion, cursor);
        RuntimeException exception = expectThrows(RuntimeException.class, () -> {
            Cursor.decodeFromString(encodedWithWrongVersion);
        });

        assertEquals(exception.getMessage(), "Unsupported scroll version " + nextMinorVersion);
    }


}
