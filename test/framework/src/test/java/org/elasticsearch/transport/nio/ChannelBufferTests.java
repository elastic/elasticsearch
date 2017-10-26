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

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesPage;
import org.elasticsearch.test.ESTestCase;

public class ChannelBufferTests extends ESTestCase {

    private BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;

    public void testChannelBufferCanBeExpanded() {
        BytesPage bytesPage = bigArrays.newBytePage();
        NetworkBytesReference reference = HeapNetworkBytes.fromBytesPage(bytesPage);
        ChannelBuffer channelBuffer = new ChannelBuffer(reference);

        assertEquals(BigArrays.BYTE_PAGE_SIZE, channelBuffer.length());

        BytesPage bytesPage2 = bigArrays.newBytePage();
        NetworkBytesReference reference2 = HeapNetworkBytes.fromBytesPage(bytesPage2);
        channelBuffer.addBuffer(reference2);

        assertEquals(BigArrays.BYTE_PAGE_SIZE * 2, channelBuffer.length());
    }

    public void testChannelBufferCanBeAccessedUsingOffsets() {
        BytesPage bytesPage = bigArrays.newBytePage();
        bytesPage.getByteArray()[0] = (byte) 10;
        NetworkBytesReference reference = HeapNetworkBytes.fromBytesPage(bytesPage);
        ChannelBuffer channelBuffer = new ChannelBuffer(reference);

        assertEquals((byte) 10, channelBuffer.get(0));

        BytesPage bytesPage2 = bigArrays.newBytePage();
        bytesPage2.getByteArray()[0] = (byte) 11;
        NetworkBytesReference reference2 = HeapNetworkBytes.fromBytesPage(bytesPage2);
        channelBuffer.addBuffer(reference2);

        assertEquals((byte) 11, channelBuffer.get(BigArrays.BYTE_PAGE_SIZE));
    }

    public void testChannelBufferModifiesUnderlyingIndexes() {
        BytesPage bytesPage = bigArrays.newBytePage();
        NetworkBytesReference reference = HeapNetworkBytes.fromBytesPage(bytesPage);
        ChannelBuffer channelBuffer = new ChannelBuffer(reference);

        assertEquals(0, reference.getIndex());

        BytesPage bytesPage2 = bigArrays.newBytePage();
        NetworkBytesReference reference2 = HeapNetworkBytes.fromBytesPage(bytesPage2);
        channelBuffer.addBuffer(reference2);

        assertEquals(0, reference2.getIndex());

        int randomDelta = randomInt(200);
        channelBuffer.incrementIndex(BigArrays.BYTE_PAGE_SIZE - randomDelta);

        assertEquals(BigArrays.BYTE_PAGE_SIZE - randomDelta, channelBuffer.getIndex());
        assertEquals(BigArrays.BYTE_PAGE_SIZE - randomDelta, reference.getIndex());
        assertEquals(0, reference2.getIndex());

        channelBuffer.incrementIndex(randomDelta + 1);

        assertEquals(BigArrays.BYTE_PAGE_SIZE + 1, channelBuffer.getIndex());
        assertEquals(BigArrays.BYTE_PAGE_SIZE, reference.getIndex());
        assertEquals(1, reference2.getIndex());
    }
}
