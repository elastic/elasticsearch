/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.action.BasicFormatter;
import org.elasticsearch.xpack.sql.action.SqlQueryResponse;
import org.elasticsearch.xpack.sql.execution.search.SearchHitCursorTests;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.Cursors;
import org.elasticsearch.xpack.sql.session.CursorsTestUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.action.support.PlainActionFuture.newFuture;
import static org.elasticsearch.xpack.sql.proto.SqlVersion.DATE_NANOS_SUPPORT_VERSION;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class CursorTests extends ESTestCase {

    public void testEmptyCursorClearCursor() {
        Client clientMock = mock(Client.class);
        Cursor cursor = Cursor.EMPTY;
        PlainActionFuture<Boolean> future = newFuture();
        cursor.clear(clientMock, writableRegistry(), future);
        assertFalse(future.actionGet());
        verifyNoMoreInteractions(clientMock);
    }

    private static SqlQueryResponse createRandomSqlResponse() {
        int columnCount = between(1, 10);

        List<ColumnInfo> columns = null;
        if (randomBoolean()) {
            columns = new ArrayList<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                columns.add(new ColumnInfo(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10), randomInt(25)));
            }
        }
        return new SqlQueryResponse("", randomFrom(Mode.values()), DATE_NANOS_SUPPORT_VERSION, false, columns, Collections.emptyList());
    }

    @SuppressWarnings("unchecked")
    static Cursor randomNonEmptyCursor() {
        Supplier<Cursor> cursorSupplier = randomFrom(SearchHitCursorTests::randomSearchHitCursor, () -> {
            SqlQueryResponse response = createRandomSqlResponse();
            if (response.columns() != null && response.rows() != null) {
                return new TextFormatterCursor(
                    SearchHitCursorTests.randomSearchHitCursor(),
                    new BasicFormatter(response.columns(), response.rows(), BasicFormatter.FormatOption.CLI)
                );
            } else {
                return SearchHitCursorTests.randomSearchHitCursor();
            }
        }, () -> {
            SqlQueryResponse response = createRandomSqlResponse();
            if (response.columns() != null && response.rows() != null) {
                return new TextFormatterCursor(
                    SearchHitCursorTests.randomSearchHitCursor(),
                    new BasicFormatter(response.columns(), response.rows(), BasicFormatter.FormatOption.TEXT)
                );
            } else {
                return SearchHitCursorTests.randomSearchHitCursor();
            }
        });
        return cursorSupplier.get();
    }

    public void testVersionHandling() {
        Cursor cursor = randomNonEmptyCursor();
        assertEquals(cursor, decodeFromString(Cursors.encodeToString(cursor, randomZone())));

        Version nextMinorVersion = Version.fromId(Version.CURRENT.id + 10000);

        String encodedWithWrongVersion = CursorsTestUtil.encodeToString(cursor, nextMinorVersion, randomZone());
        SqlIllegalArgumentException exception = expectThrows(
            SqlIllegalArgumentException.class,
            () -> decodeFromString(encodedWithWrongVersion)
        );

        assertEquals(
            LoggerMessageFormat.format("Unsupported cursor version [{}], expected [{}]", nextMinorVersion, Version.CURRENT),
            exception.getMessage()
        );
    }

    public static Cursor decodeFromString(String base64) {
        return Cursors.decodeFromStringWithZone(base64).v1();
    }
}
