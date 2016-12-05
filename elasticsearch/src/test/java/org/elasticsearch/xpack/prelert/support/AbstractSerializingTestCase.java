/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.support;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

public abstract class AbstractSerializingTestCase<T extends ToXContent & Writeable> extends AbstractWireSerializingTestCase<T> {

    /**
     * Generic test that creates new instance from the test instance and checks
     * both for equality and asserts equality on the two queries.
     */
    public void testFromXContent() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTQUERIES; runs++) {
            T testInstance = createTestInstance();
            XContentBuilder builder = toXContent(testInstance, randomFrom(XContentType.values()));
            XContentBuilder shuffled = shuffleXContent(builder, shuffleProtectedFields());
            assertParsedInstance(shuffled.bytes(), testInstance);
            for (Map.Entry<String, T> alternateVersion : getAlternateVersions().entrySet()) {
                String instanceAsString = alternateVersion.getKey();
                assertParsedInstance(new BytesArray(instanceAsString), alternateVersion.getValue());
            }
        }
    }

    private void assertParsedInstance(BytesReference queryAsBytes, T expectedInstance)
            throws IOException {
        XContentParser parser = XContentFactory.xContent(queryAsBytes).createParser(queryAsBytes);
        T newInstance = parseQuery(parser, ParseFieldMatcher.STRICT);
        assertNotSame(newInstance, expectedInstance);
        assertEquals(expectedInstance, newInstance);
        assertEquals(expectedInstance.hashCode(), newInstance.hashCode());
    }

    private T parseQuery(XContentParser parser, ParseFieldMatcher matcher) throws IOException {
        T parsedInstance = parseInstance(parser, matcher);
        assertNull(parser.nextToken());
        return parsedInstance;
    }

    protected abstract T parseInstance(XContentParser parser, ParseFieldMatcher matcher);

    /**
     * Subclasses can override this method and return an array of fieldnames
     * which should be protected from recursive random shuffling in the
     * {@link #testFromXContent()} test case
     */
    protected String[] shuffleProtectedFields() {
        return Strings.EMPTY_ARRAY;
    }

    protected static <T extends ToXContent & Writeable> XContentBuilder toXContent(T instance, XContentType contentType)
            throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(contentType);
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        instance.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return builder;
    }

    /**
     * Returns alternate string representation of the query that need to be
     * tested as they are never used as output of the test instance. By default
     * there are no alternate versions.
     */
    protected Map<String, T> getAlternateVersions() {
        return Collections.emptyMap();
    }

}
