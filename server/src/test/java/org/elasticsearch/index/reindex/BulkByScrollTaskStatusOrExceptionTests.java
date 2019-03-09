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

package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.index.reindex.BulkByScrollTask.StatusOrException;

import java.io.IOException;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;

public class BulkByScrollTaskStatusOrExceptionTests extends AbstractXContentTestCase<StatusOrException> {
    @Override
    protected StatusOrException createTestInstance() {
        // failures are tested separately, so we can test XContent equivalence at least when we have no failures
        return createTestInstanceWithoutExceptions();
    }

    static StatusOrException createTestInstanceWithoutExceptions() {
        return new StatusOrException(BulkByScrollTaskStatusTests.randomStatusWithoutException());
    }

    static StatusOrException createTestInstanceWithExceptions() {
        if (randomBoolean()) {
            return new StatusOrException(new ElasticsearchException("test_exception"));
        } else {
            return new StatusOrException(BulkByScrollTaskStatusTests.randomStatus());
        }
    }

    @Override
    protected StatusOrException doParseInstance(XContentParser parser) throws IOException {
        return StatusOrException.fromXContent(parser);
    }

    public static void assertEqualStatusOrException(StatusOrException expected, StatusOrException actual,
                                                    boolean includeUpdated, boolean includeCreated) {
        if (expected != null && actual != null) {
            assertNotSame(expected, actual);
            if (expected.getException() == null) {
                BulkByScrollTaskStatusTests
                    // we test includeCreated params in the Status tests
                    .assertEqualStatus(expected.getStatus(), actual.getStatus(), includeUpdated, includeCreated);
            } else {
                assertThat(
                    actual.getException().getMessage(),
                    containsString(expected.getException().getMessage())
                );
            }
        } else {
            // If one of them is null both of them should be null
            assertSame(expected, actual);
        }
    }

    @Override
    protected void assertEqualInstances(StatusOrException expected, StatusOrException actual) {
        assertEqualStatusOrException(expected, actual, true, true);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    /**
     * Test parsing {@link StatusOrException} with inner failures as they don't support asserting on xcontent equivalence, given that
     * exceptions are not parsed back as the same original class. We run the usual {@link AbstractXContentTestCase#testFromXContent()}
     * without failures, and this other test with failures where we disable asserting on xcontent equivalence at the end.
     */
    public void testFromXContentWithFailures() throws IOException {
        Supplier<StatusOrException> instanceSupplier = BulkByScrollTaskStatusOrExceptionTests::createTestInstanceWithExceptions;
        //with random fields insertion in the inner exceptions, some random stuff may be parsed back as metadata,
        //but that does not bother our assertions, as we only want to test that we don't break.
        boolean supportsUnknownFields = true;
        //exceptions are not of the same type whenever parsed back
        boolean assertToXContentEquivalence = false;
        AbstractXContentTestCase.testFromXContent(NUMBER_OF_TEST_RUNS, instanceSupplier, supportsUnknownFields, Strings.EMPTY_ARRAY,
            getRandomFieldsExcludeFilter(), this::createParser, this::doParseInstance,
            this::assertEqualInstances, assertToXContentEquivalence, ToXContent.EMPTY_PARAMS);
    }
}
