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

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

/**
 * @deprecated Use {@link AbstractResponseTestCase} instead of this class.
 */
// TODO: Remove and change subclasses to use AbstractResponseTestCase instead
@Deprecated
public abstract class AbstractHlrcWriteableXContentTestCase<T extends ToXContent & Writeable, H>
        extends AbstractSerializingTestCase<T> {

    /**
     * Generic test that creates new instance of HLRC request/response from the test instance and checks
     * both for equality and asserts equality on the two queries.
     */
    public final void testHlrcFromXContent() throws IOException {
        xContentTester(this::createParser, this::createTestInstance, getToXContentParams(),
            p -> convertHlrcToInternal(doHlrcParseInstance(p)))
            .numberOfTestRuns(NUMBER_OF_TEST_RUNS)
            .supportsUnknownFields(supportsUnknownFields())
            .shuffleFieldsExceptions(getShuffleFieldsExceptions())
            .randomFieldsExcludeFilter(getRandomFieldsExcludeFilter())
            .assertEqualsConsumer(this::assertEqualInstances)
            .assertToXContentEquivalence(true)
            .test();
    }

    /**
     * Parses to a new HLRC instance using the provided {@link XContentParser}
     */
    public abstract H doHlrcParseInstance(XContentParser parser) throws IOException;

    /**
     * Converts a HLRC instance to a XPack instance
     */
    public abstract T convertHlrcToInternal(H instance);

    //TODO this would be final ideally: why do both responses need to parse from xcontent, only one (H) should? I think that T#fromXContent
    //are only there for testing and could go away? Then the additional testHlrcFromXContent is also no longer needed.
    @Override
    protected T doParseInstance(XContentParser parser) throws IOException {
        return convertHlrcToInternal(doHlrcParseInstance(parser));
    }
}
