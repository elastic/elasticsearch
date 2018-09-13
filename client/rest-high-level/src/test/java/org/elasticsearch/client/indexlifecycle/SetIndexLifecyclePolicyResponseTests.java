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

package org.elasticsearch.client.indexlifecycle;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SetIndexLifecyclePolicyResponseTests extends AbstractStreamableXContentTestCase<SetIndexLifecyclePolicyResponse> {

    @Override
    protected SetIndexLifecyclePolicyResponse createBlankInstance() {
        return new SetIndexLifecyclePolicyResponse();
    }

    @Override
    protected SetIndexLifecyclePolicyResponse createTestInstance() {
        List<String> failedIndexes = Arrays.asList(generateRandomStringArray(20, 20, false));
        return new SetIndexLifecyclePolicyResponse(failedIndexes);
    }

    @Override
    protected SetIndexLifecyclePolicyResponse mutateInstance(SetIndexLifecyclePolicyResponse instance) throws IOException {
        List<String> failedIndices = randomValueOtherThan(instance.getFailedIndexes(),
                () -> Arrays.asList(generateRandomStringArray(20, 20, false)));
        return new SetIndexLifecyclePolicyResponse(failedIndices);
    }

    @Override
    protected SetIndexLifecyclePolicyResponse doParseInstance(XContentParser parser) throws IOException {
        return SetIndexLifecyclePolicyResponse.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public void testNullFailedIndices() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new SetIndexLifecyclePolicyResponse(null));
        assertEquals("failed_indexes cannot be null", exception.getMessage());
    }

    public void testHasFailures() {
        SetIndexLifecyclePolicyResponse response = new SetIndexLifecyclePolicyResponse(new ArrayList<>());
        assertFalse(response.hasFailures());
        assertEquals(Collections.emptyList(), response.getFailedIndexes());

        int size = randomIntBetween(1, 10);
        List<String> failedIndexes = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            failedIndexes.add(randomAlphaOfLength(20));
        }
        response = new SetIndexLifecyclePolicyResponse(failedIndexes);
        assertTrue(response.hasFailures());
        assertEquals(failedIndexes, response.getFailedIndexes());
    }

}
