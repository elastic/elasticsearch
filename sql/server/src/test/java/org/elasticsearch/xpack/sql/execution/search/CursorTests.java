/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.mockito.ArgumentCaptor;

import java.util.Collections;

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

}
