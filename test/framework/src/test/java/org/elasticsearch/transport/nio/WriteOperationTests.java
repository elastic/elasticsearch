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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.nio.channel.NioChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.junit.Before;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WriteOperationTests extends ESTestCase {

    private NioSocketChannel channel;
    private ActionListener<Void> listener;

    @Before
    @SuppressWarnings("unchecked")
    public void setFields() {
        channel = mock(NioSocketChannel.class);
        listener = mock(ActionListener.class);

    }

    public void testFlush() throws IOException {
        WriteOperation writeOp = new WriteOperation(channel, new BytesArray(new byte[10]), listener);


        when(channel.write(any())).thenAnswer(invocationOnMock -> {
            NetworkBytesReference[] refs = (NetworkBytesReference[]) invocationOnMock.getArguments()[0];
            refs[0].incrementRead(10);
            return 10;
        });

        writeOp.flush();

        assertTrue(writeOp.isFullyFlushed());
    }

    public void testPartialFlush() throws IOException {
        WriteOperation writeOp = new WriteOperation(channel, new BytesArray(new byte[10]), listener);

        when(channel.write(any())).thenAnswer(invocationOnMock -> {
            NetworkBytesReference[] refs = (NetworkBytesReference[]) invocationOnMock.getArguments()[0];
            refs[0].incrementRead(5);
            return 5;
        });

        writeOp.flush();

        assertFalse(writeOp.isFullyFlushed());
        assertEquals(5, writeOp.getByteReferences()[0].getReadRemaining());
    }
}
