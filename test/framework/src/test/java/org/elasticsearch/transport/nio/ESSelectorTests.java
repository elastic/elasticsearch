/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport.nio;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.nio.channel.NioChannel;
import org.junit.Before;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ESSelectorTests extends ESTestCase {

    private ESSelector selector;
    private EventHandler handler;
    private Selector rawSelector;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        handler = mock(EventHandler.class);
        rawSelector = mock(Selector.class);
        selector = new TestSelector(handler, rawSelector);
    }

    public void testQueueChannelForClosed() throws IOException {
        NioChannel channel = mock(NioChannel.class);
        when(channel.getSelector()).thenReturn(selector);

        selector.queueChannelClose(channel);

        selector.singleLoop();

        verify(handler).handleClose(channel);
    }

    public void testSelectorClosedExceptionIsNotCaughtWhileRunning() throws IOException {
        boolean closedSelectorExceptionCaught = false;
        when(rawSelector.select(anyInt())).thenThrow(new ClosedSelectorException());
        try {
            this.selector.singleLoop();
        } catch (ClosedSelectorException e) {
            closedSelectorExceptionCaught = true;
        }

        assertTrue(closedSelectorExceptionCaught);
    }

    public void testIOExceptionWhileSelect() throws IOException {
        IOException ioException = new IOException();

        when(rawSelector.select(anyInt())).thenThrow(ioException);

        this.selector.singleLoop();

        verify(handler).selectException(ioException);
    }

    private static class TestSelector extends ESSelector {

        TestSelector(EventHandler eventHandler, Selector selector) throws IOException {
            super(eventHandler, selector);
        }

        @Override
        void processKey(SelectionKey selectionKey) throws CancelledKeyException {

        }

        @Override
        void preSelect() {

        }

        @Override
        void cleanup() {

        }
    }

}
