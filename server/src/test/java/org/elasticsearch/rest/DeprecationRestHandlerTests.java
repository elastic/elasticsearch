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
package org.elasticsearch.rest;

import com.carrotsearch.randomizedtesting.generators.CodepointSetGenerator;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.test.ESTestCase;

import org.junit.Before;
import org.mockito.InOrder;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link DeprecationRestHandler}.
 */
public class DeprecationRestHandlerTests extends ESTestCase {

    private RestHandler handler;
    /**
     * Note: Headers should only use US ASCII (and this inevitably becomes one!).
     */
    private final String deprecationMessage = randomAlphaOfLengthBetween(1, 30);
    private DeprecationLogger deprecationLogger;

    @Before
    public void setup() {
        handler = mock(RestHandler.class);
        deprecationLogger = mock(DeprecationLogger.class);
    }

    public void testNullHandler() {
        expectThrows(NullPointerException.class, () -> new DeprecationRestHandler(null, deprecationMessage, deprecationLogger));
    }

    public void testInvalidDeprecationMessageThrowsException() {
        String invalidDeprecationMessage = randomFrom("", null, "     ");

        expectThrows(IllegalArgumentException.class,
                     () -> new DeprecationRestHandler(handler, invalidDeprecationMessage, deprecationLogger));
    }

    public void testNullDeprecationLogger() {
        expectThrows(NullPointerException.class, () -> new DeprecationRestHandler(handler, deprecationMessage, null));
    }

    public void testHandleRequestLogsWarningThenForwards() throws Exception {
        RestRequest request = mock(RestRequest.class);
        RestChannel channel = mock(RestChannel.class);
        NodeClient client = mock(NodeClient.class);

        DeprecationRestHandler deprecatedHandler = new DeprecationRestHandler(handler, deprecationMessage, deprecationLogger);

        // test it
        deprecatedHandler.handleRequest(request, channel, client);

        InOrder inOrder = inOrder(handler, request, channel, deprecationLogger);

        // log, then forward
        inOrder.verify(deprecationLogger).deprecatedAndMaybeLog("deprecated_route", deprecationMessage);
        inOrder.verify(handler).handleRequest(request, channel, client);
        inOrder.verifyNoMoreInteractions();
    }

    public void testValidHeaderValue() {
        ASCIIHeaderGenerator generator = new ASCIIHeaderGenerator();
        String value = generator.ofCodeUnitsLength(random(), 1, 50);

        if (value.trim().length() == 0) {
            // empty text, not a valid header
            assertFalse(DeprecationRestHandler.validHeaderValue(value));
            Exception e = expectThrows(IllegalArgumentException.class, () -> DeprecationRestHandler.requireValidHeader(value));
            assertEquals("header value must contain only US ASCII text", e.getMessage());
        } else {
            assertTrue(DeprecationRestHandler.validHeaderValue(value));
            assertSame(value, DeprecationRestHandler.requireValidHeader(value));
        }
    }

    public void testInvalidHeaderValue() {
        ASCIIHeaderGenerator generator = new ASCIIHeaderGenerator();
        String value = generator.ofCodeUnitsLength(random(), 0, 25) +
                       randomFrom('\t', '\0', '\n', (char)27 /* ESC */, (char)31 /* unit separator*/, (char)127 /* DEL */) +
                       generator.ofCodeUnitsLength(random(), 0, 25);

        assertFalse(DeprecationRestHandler.validHeaderValue(value));

        expectThrows(IllegalArgumentException.class, () -> DeprecationRestHandler.requireValidHeader(value));
    }

    public void testInvalidHeaderValueNull() {
        assertFalse(DeprecationRestHandler.validHeaderValue(null));

        expectThrows(IllegalArgumentException.class, () -> DeprecationRestHandler.requireValidHeader(null));
    }

    public void testInvalidHeaderValueEmpty() {
        String blank = randomFrom("", "\t", "    ");

        assertFalse(DeprecationRestHandler.validHeaderValue(blank));

        expectThrows(IllegalArgumentException.class, () -> DeprecationRestHandler.requireValidHeader(blank));
    }

    public void testSupportsContentStreamTrue() {
        when(handler.supportsContentStream()).thenReturn(true);
        assertTrue(new DeprecationRestHandler(handler, deprecationMessage, deprecationLogger).supportsContentStream());
    }

    public void testSupportsContentStreamFalse() {
        when(handler.supportsContentStream()).thenReturn(false);
        assertFalse(new DeprecationRestHandler(handler, deprecationMessage, deprecationLogger).supportsContentStream());
    }

    /**
     * {@code ASCIIHeaderGenerator} only uses characters expected to be valid in headers (simplified US-ASCII).
     */
    private static class ASCIIHeaderGenerator extends CodepointSetGenerator {
        /**
         * Create a character array for characters [{@code from}, {@code to}].
         *
         * @param from Starting code point (inclusive).
         * @param to Ending code point (inclusive).
         * @return Never {@code null}.
         */
        static char[] asciiFromTo(int from, int to) {
            char[] chars = new char[to - from + 1];

            for (int i = from; i <= to; ++i) {
                chars[i - from] = (char)i;
            }

            return chars;
        }

        /**
         * Create a generator for characters [32, 126].
         */
        ASCIIHeaderGenerator() {
            super(asciiFromTo(32, 126));
        }
    }

}
