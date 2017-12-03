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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public abstract class AbstractSerializingTestCase<T extends ToXContent & Writeable> extends AbstractWireSerializingTestCase<T> {

    /**
     * Generic test that creates new instance from the test instance and checks
     * both for equality and asserts equality on the two instances.
     */
    public void testFromXContent() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            T testInstance = createTestInstance();
            XContentType xContentType = randomFrom(XContentType.values());
            BytesReference shuffled = toShuffledXContent(testInstance, xContentType, ToXContent.EMPTY_PARAMS,
                    false, getShuffleFieldsExceptions());
            assertParsedInstance(xContentType, shuffled, testInstance);
        }
    }

    protected void assertParsedInstance(XContentType xContentType, BytesReference instanceAsBytes, T expectedInstance)
            throws IOException {

        XContentParser parser = createParser(XContentFactory.xContent(xContentType), instanceAsBytes);
        T newInstance = parseInstance(parser);
        assertNotSame(newInstance, expectedInstance);
        assertEquals(expectedInstance, newInstance);
        assertEquals(expectedInstance.hashCode(), newInstance.hashCode());
    }

    protected T parseInstance(XContentParser parser) throws IOException {
        T parsedInstance = doParseInstance(parser);
        assertNull(parser.nextToken());
        return parsedInstance;
    }

    /**
     * Parses to a new instance using the provided {@link XContentParser}
     */
    protected abstract T doParseInstance(XContentParser parser) throws IOException;

    /**
     * Fields that have to be ignored when shuffling as part of testFromXContent
     */
    protected String[] getShuffleFieldsExceptions() {
        return Strings.EMPTY_ARRAY;
    }
}
