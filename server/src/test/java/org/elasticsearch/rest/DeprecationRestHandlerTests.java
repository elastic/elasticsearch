/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.rest;

import com.carrotsearch.randomizedtesting.generators.CodepointSetGenerator;

import org.apache.logging.log4j.Level;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.InOrder;

import java.util.List;

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
    private static final RestRequest.Method METHOD = RestRequest.Method.GET;
    private static final String PATH = "/some/path";

    @Before
    public void setup() {
        handler = mock(RestHandler.class);
        deprecationLogger = mock(DeprecationLogger.class);
    }

    public void testNullHandler() {
        expectThrows(
            NullPointerException.class,
            () -> new DeprecationRestHandler(null, METHOD, PATH, null, deprecationMessage, deprecationLogger, false)
        );
    }

    public void testInvalidDeprecationMessageThrowsException() {
        String invalidDeprecationMessage = randomFrom("", null, "     ");

        expectThrows(
            IllegalArgumentException.class,
            () -> new DeprecationRestHandler(handler, METHOD, PATH, null, invalidDeprecationMessage, deprecationLogger, false)
        );
    }

    public void testNullDeprecationLogger() {
        expectThrows(
            NullPointerException.class,
            () -> new DeprecationRestHandler(handler, METHOD, PATH, null, deprecationMessage, null, false)
        );
    }

    public void testHandleRequestLogsThenForwards() throws Exception {
        for (boolean compatibleVersionWarning : List.of(true, false)) {
            RestRequest request = mock(RestRequest.class);
            RestChannel channel = mock(RestChannel.class);
            NodeClient client = mock(NodeClient.class);

            final Level deprecationLevel = randomFrom(Level.WARN, DeprecationLogger.CRITICAL);

            DeprecationRestHandler deprecatedHandler = new DeprecationRestHandler(
                handler,
                METHOD,
                PATH,
                deprecationLevel,
                deprecationMessage,
                deprecationLogger,
                compatibleVersionWarning
            );

            // test it
            deprecatedHandler.handleRequest(request, channel, client);

            InOrder inOrder = inOrder(handler, request, channel, deprecationLogger);

            // log, then forward
            if (compatibleVersionWarning) {
                if (deprecationLevel == null || deprecationLevel == DeprecationLogger.CRITICAL) {
                    inOrder.verify(deprecationLogger).compatibleCritical("deprecated_route_GET_/some/path", deprecationMessage);
                } else {
                    inOrder.verify(deprecationLogger).compatible(Level.WARN, "deprecated_route_GET_/some/path", deprecationMessage);
                }
            } else {
                if (deprecationLevel == null || deprecationLevel == Level.WARN) {
                    inOrder.verify(deprecationLogger).warn(DeprecationCategory.API, "deprecated_route_GET_/some/path", deprecationMessage);
                } else {
                    inOrder.verify(deprecationLogger)
                        .critical(DeprecationCategory.API, "deprecated_route_GET_/some/path", deprecationMessage);
                }
            }

            inOrder.verify(handler).handleRequest(request, channel, client);
            inOrder.verifyNoMoreInteractions();
        }
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
        String value = generator.ofCodeUnitsLength(random(), 0, 25) + randomFrom(
            '\t',
            '\0',
            '\n',
            (char) 27 /* ESC */,
            (char) 31 /* unit separator*/,
            (char) 127 /* DEL */
        ) + generator.ofCodeUnitsLength(random(), 0, 25);

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

    public void testSupportsBulkContentTrue() {
        when(handler.supportsBulkContent()).thenReturn(true);
        assertTrue(
            new DeprecationRestHandler(handler, METHOD, PATH, Level.WARN, deprecationMessage, deprecationLogger, false)
                .supportsBulkContent()
        );
    }

    public void testSupportsBulkContentFalse() {
        when(handler.supportsBulkContent()).thenReturn(false);
        assertFalse(
            new DeprecationRestHandler(handler, METHOD, PATH, Level.WARN, deprecationMessage, deprecationLogger, false)
                .supportsBulkContent()
        );
    }

    public void testDeprecationLevel() {
        DeprecationRestHandler handler = new DeprecationRestHandler(
            this.handler,
            METHOD,
            PATH,
            Level.WARN,
            deprecationMessage,
            deprecationLogger,
            false
        );
        assertEquals(Level.WARN, handler.getDeprecationLevel());

        handler = new DeprecationRestHandler(
            this.handler,
            METHOD,
            PATH,
            DeprecationLogger.CRITICAL,
            deprecationMessage,
            deprecationLogger,
            false
        );
        assertEquals(DeprecationLogger.CRITICAL, handler.getDeprecationLevel());

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new DeprecationRestHandler(this.handler, METHOD, PATH, null, deprecationMessage, deprecationLogger, false)
        );
        assertEquals(exception.getMessage(), "unexpected deprecation logger level: null, expected either 'CRITICAL' or 'WARN'");

        exception = expectThrows(
            IllegalArgumentException.class,
            () -> new DeprecationRestHandler(this.handler, METHOD, PATH, Level.OFF, deprecationMessage, deprecationLogger, false)
        );
        assertEquals(exception.getMessage(), "unexpected deprecation logger level: OFF, expected either 'CRITICAL' or 'WARN'");
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
                chars[i - from] = (char) i;
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
