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
package org.elasticsearch.test;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public abstract class AbstractStreamableXContentTestCase<T extends ToXContent & Streamable> extends AbstractStreamableTestCase<T> {

    /**
     * Generic test that creates new instance from the test instance and checks
     * both for equality and asserts equality on the two queries.
     */
    public void testFromXContent() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            T testInstance = createTestInstance();
            XContentType xContentType = randomFrom(XContentType.values());
            XContentBuilder builder = toXContent(testInstance, xContentType);
            XContentBuilder shuffled = shuffleXContent(builder);
            assertParsedInstance(xContentType, shuffled.bytes(), testInstance);
            for (Map.Entry<String, T> alternateVersion : getAlternateVersions().entrySet()) {
                String instanceAsString = alternateVersion.getKey();
                assertParsedInstance(XContentType.JSON, new BytesArray(instanceAsString), alternateVersion.getValue());
            }
        }
    }

    private void assertParsedInstance(XContentType xContentType, BytesReference instanceAsBytes, T expectedInstance)
            throws IOException {
        XContentParser parser = createParser(XContentFactory.xContent(xContentType), instanceAsBytes);
        T newInstance = parseInstance(parser);
        assertNotSame(newInstance, expectedInstance);
        assertEquals(expectedInstance, newInstance);
        assertEquals(expectedInstance.hashCode(), newInstance.hashCode());
    }

    private T parseInstance(XContentParser parser) throws IOException {
        T parsedInstance = doParseInstance(parser);
        assertNull(parser.nextToken());
        return parsedInstance;
    }

    /**
     * Parses to a new instance using the provided {@link XContentParser}
     */
    protected abstract T doParseInstance(XContentParser parser);

    /**
     * Renders the provided instance in XContent
     * 
     * @param instance
     *            the instance to render
     * @param contentType
     *            the content type to render to
     */
    protected static <T extends ToXContent> XContentBuilder toXContent(T instance, XContentType contentType)
            throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(contentType);
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        instance.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return builder;
    }

    /**
     * Returns alternate string representation of the instance that need to be
     * tested as they are never used as output of the test instance. By default
     * there are no alternate versions.
     * 
     * These alternatives must be JSON strings.
     */
    protected Map<String, T> getAlternateVersions() {
        return Collections.emptyMap();
    }
}
