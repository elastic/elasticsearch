/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * Base class for HLRC request parsing tests.
 *
 * This case class facilitates generating client side request test instances and
 * verifies that they are correctly parsed into server side request instances.
 *
 * @param <C> The class representing the request on the client side.
 * @param <S> The class representing the request on the server side.
 */
public abstract class AbstractRequestTestCase<C extends ToXContent, S> extends ESTestCase {

    public final void testFromXContent() throws IOException {
        final C clientTestInstance = createClientTestInstance();

        final XContentType xContentType = randomFrom(XContentType.values());
        final BytesReference bytes = toShuffledXContent(clientTestInstance, xContentType, ToXContent.EMPTY_PARAMS, randomBoolean());

        final XContent xContent = XContentFactory.xContent(xContentType);
        final XContentParser parser = xContent.createParser(
            xContentRegistry(),
            LoggingDeprecationHandler.INSTANCE,
            bytes.streamInput());
        final S serverInstance = doParseToServerInstance(parser);
        assertInstances(serverInstance, clientTestInstance);
    }

    /**
     * The {@link NamedXContentRegistry} to use for this test. Subclasses may override this to have a more realistic registry.
     */
    protected NamedXContentRegistry xContentRegistry() {
        return NamedXContentRegistry.EMPTY;
    }

    /**
     * @return The client test instance to be serialized to xcontent as bytes
     */
    protected abstract C createClientTestInstance();

    /**
     * @param parser The xcontent parser
     * @return The server side instance that is parsed from the xcontent which originates from the client side test instance
     */
    protected abstract S doParseToServerInstance(XContentParser parser) throws IOException;

    /**
     * Assert that the server instance and client test instance contain the same content.
     * Typically by asserting whether each property of both instances are equal to each other.
     *
     * @param serverInstance        The server side instance that was created by {@link #doParseToServerInstance(XContentParser)}
     * @param clientTestInstance    The client side test instance that was created by {@link #createClientTestInstance()}
     */
    protected abstract void assertInstances(S serverInstance, C clientTestInstance);

}
