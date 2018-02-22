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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.function.Predicate;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public abstract class AbstractXContentTestCase<T extends ToXContent> extends ESTestCase {

    /**
     * Generic test that creates new instance from the test instance and checks
     * both for equality and asserts equality on the two queries.
     */
    public final void testFromXContent() throws IOException {
        for (int runs = 0; runs < numberOfTestRuns(); runs++) {
            T testInstance = createTestInstance();
            XContentType xContentType = randomFrom(XContentType.values());
            BytesReference shuffled = toShuffledXContent(testInstance, xContentType, ToXContent.EMPTY_PARAMS,
                    false, getShuffleFieldsExceptions());
            BytesReference withRandomFields;
            if (supportsUnknownFields()) {
                // we add a few random fields to check that parser is lenient on new fields
                withRandomFields = XContentTestUtils.insertRandomFields(xContentType, shuffled, getRandomFieldsExcludeFilter(), random());
            } else {
                withRandomFields = shuffled;
            }
            XContentParser parser = createParser(XContentFactory.xContent(xContentType), withRandomFields);
            T parsed = parseInstance(parser);
            T expected = getExpectedFromXContent(testInstance);
            assertEqualInstances(expected, parsed);
            assertToXContentEquivalent(shuffled, XContentHelper.toXContent(parsed, xContentType, false), xContentType);
        }
    }

    protected abstract int numberOfTestRuns();

    /**
     * Creates a random test instance to use in the tests. This method will be
     * called multiple times during test execution and should return a different
     * random instance each time it is called.
     */
    protected abstract T createTestInstance();

    private T parseInstance(XContentParser parser) throws IOException {
        T parsedInstance = doParseInstance(parser);
        assertNull(parser.nextToken());
        return parsedInstance;
    }

    /**
     * Parses to a new instance using the provided {@link XContentParser}
     */
    protected abstract T doParseInstance(XContentParser parser) throws IOException;

    /**
     * Returns the expected parsed object given the test object that the parser will be fed with.
     * Useful in cases some fields are not written as part of toXContent, hence not parsed back.
     */
    protected T getExpectedFromXContent(T testInstance) {
        return testInstance;
    }

    protected void assertEqualInstances(T expectedInstance, T newInstance) {
        assertNotSame(newInstance, expectedInstance);
        assertEquals(expectedInstance, newInstance);
        assertEquals(expectedInstance.hashCode(), newInstance.hashCode());
    }

    /**
     * Indicates whether the parser supports unknown fields or not. In case it does, such behaviour will be tested by
     * inserting random fields before parsing and checking that they don't make parsing fail.
     */
    protected abstract boolean supportsUnknownFields();

    protected Predicate<String> getRandomFieldsExcludeFilter() {
        assert supportsUnknownFields();
        return field -> false;
    }

    /**
     * Fields that have to be ignored when shuffling as part of testFromXContent
     */
    protected String[] getShuffleFieldsExceptions() {
        return Strings.EMPTY_ARRAY;
    }
}
