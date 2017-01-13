/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.support;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchModule;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public abstract class AbstractStreamableXContentTestCase<T extends ToXContent & Streamable> extends AbstractStreamableTestCase<T> {

    protected static final NamedXContentRegistry NAMED_X_CONTENT_REGISTRY;
    static {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        NAMED_X_CONTENT_REGISTRY = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

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

    private void assertParsedInstance(BytesReference queryAsBytes, T expectedInstance) throws IOException {
        XContentParser parser = XContentFactory.xContent(queryAsBytes).createParser(NAMED_X_CONTENT_REGISTRY, queryAsBytes);
        T newInstance = parseQuery(parser);
        assertNotSame(newInstance, expectedInstance);
        assertEquals(expectedInstance, newInstance);
        assertEquals(expectedInstance.hashCode(), newInstance.hashCode());
    }

    private T parseQuery(XContentParser parser) throws IOException {
        T parsedInstance = parseInstance(parser);
        assertNull(parser.nextToken());
        return parsedInstance;
    }

    protected abstract T parseInstance(XContentParser parser);

    /**
     * Subclasses can override this method and return an array of fieldnames
     * which should be protected from recursive random shuffling in the
     * {@link #testFromXContent()} test case
     */
    protected String[] shuffleProtectedFields() {
        return Strings.EMPTY_ARRAY;
    }

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
     * Returns alternate string representation of the query that need to be
     * tested as they are never used as output of the test instance. By default
     * there are no alternate versions.
     */
    protected Map<String, T> getAlternateVersions() {
        return Collections.emptyMap();
    }
}
