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
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            bytes.streamInput());
        final S serverInstance = doParseToServerInstance(parser);
        assertInstances(serverInstance, clientTestInstance);
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
