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

public class InboundChannelBufferTests extends ESTestCase {

    private static final int PAGE_SIZE = 1 << 14;

    public void testNewBufferHasSinglePage() {
        InboundChannelBuffer inboundBuffer = new InboundChannelBuffer();

        assertEquals(PAGE_SIZE, inboundBuffer.getCapacity());
        assertEquals(0, inboundBuffer.getIndex());
    }

    public void testExpandCapacity() {
        InboundChannelBuffer inboundBuffer = new InboundChannelBuffer();

        assertEquals(PAGE_SIZE, inboundBuffer.getCapacity());

        inboundBuffer.expandCapacity(PAGE_SIZE + 1);

        assertEquals(PAGE_SIZE * 2, inboundBuffer.getCapacity());
    }

    public void testExpandCapacityMultiplePages() {
        InboundChannelBuffer inboundBuffer = new InboundChannelBuffer();

        assertEquals(PAGE_SIZE, inboundBuffer.getCapacity());

        int multiple = randomInt(80);
        inboundBuffer.expandCapacity(PAGE_SIZE + ((multiple * PAGE_SIZE) - randomInt(500)));

        assertEquals(PAGE_SIZE * (multiple + 1), inboundBuffer.getCapacity());
    }

    public void testExpandCapacityRespectsOffset() {
        InboundChannelBuffer inboundBuffer = new InboundChannelBuffer();

        assertEquals(PAGE_SIZE, inboundBuffer.getCapacity());

        int offset = randomInt(300);

        inboundBuffer.releasePagesFromHead(offset);

        assertEquals(PAGE_SIZE - offset, inboundBuffer.getCapacity());

        inboundBuffer.expandCapacity(PAGE_SIZE + 1);

        assertEquals(PAGE_SIZE * 2 - offset, inboundBuffer.getCapacity());
    }

    public void testIncrementIndex() {
        InboundChannelBuffer inboundBuffer = new InboundChannelBuffer();

        assertEquals(0, inboundBuffer.getIndex());

        inboundBuffer.incrementIndex(10);

        assertEquals(10, inboundBuffer.getIndex());
    }

    public void testIncrementIndexWithOffset() {
        InboundChannelBuffer inboundBuffer = new InboundChannelBuffer();

        assertEquals(0, inboundBuffer.getIndex());

        inboundBuffer.releasePagesFromHead(10);
        inboundBuffer.incrementIndex(10);

        assertEquals(10, inboundBuffer.getIndex());

        inboundBuffer.releasePagesFromHead(2);
        assertEquals(8, inboundBuffer.getIndex());
    }
}
