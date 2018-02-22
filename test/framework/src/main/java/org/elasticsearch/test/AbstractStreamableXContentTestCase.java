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

import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.function.Predicate;

public abstract class AbstractStreamableXContentTestCase<T extends ToXContent & Streamable> extends AbstractStreamableTestCase<T> {

    /**
     * Generic test that creates new instance from the test instance and checks
     * both for equality and asserts equality on the two queries.
     */
    public final void testFromXContent() throws IOException {
        new AbstractXContentTestCase<T>() {
            @Override
            protected int numberOfTestRuns() {
                return NUMBER_OF_TEST_RUNS;
            }

            @Override
            protected T createTestInstance() {
                return AbstractStreamableXContentTestCase.this.createTestInstance();
            }

            @Override
            protected T doParseInstance(XContentParser parser) {
                return AbstractStreamableXContentTestCase.this.doParseInstance(parser);
            }

            @Override
            protected T getExpectedFromXContent(T testInstance) {
                return AbstractStreamableXContentTestCase.this.getExpectedFromXContent(testInstance);
            }

            @Override
            protected boolean supportsUnknownFields() {
                return AbstractStreamableXContentTestCase.this.supportsUnknownFields();
            }

            @Override
            protected Predicate<String> getRandomFieldsExcludeFilter() {
                return AbstractStreamableXContentTestCase.this.getRandomFieldsExcludeFilter();
            }
        }.testFromXContent();
    }

    /**
     * Returns the expected parsed object given the test object that the parser will be fed with.
     * Useful in cases some fields are not written as part of toXContent, hence not parsed back.
     */
    protected T getExpectedFromXContent(T testInstance) {
        return testInstance;
    }

    /**
     * Indicates whether the parser supports unknown fields or not. In case it does, such behaviour will be tested by
     * inserting random fields before parsing and checking that they don't make parsing fail.
     */
    protected boolean supportsUnknownFields() {
        return true;
    }

    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> false;
    }

    /**
     * Parses to a new instance using the provided {@link XContentParser}
     */
    protected abstract T doParseInstance(XContentParser parser);
}
