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
package org.elasticsearch.protocol.xpack.indexlifecycle;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.junit.Before;

public class MoveToStepRequestTests extends AbstractStreamableXContentTestCase<MoveToStepRequest> {

    private String index;
    private static final StepKeyTests stepKeyTests = new StepKeyTests();

    @Before
    public void setup() {
        index = randomAlphaOfLength(5);
    }

    @Override
    protected MoveToStepRequest createTestInstance() {
        return new MoveToStepRequest(index, stepKeyTests.createTestInstance(), stepKeyTests.createTestInstance());
    }

    @Override
    protected MoveToStepRequest createBlankInstance() {
        return new MoveToStepRequest();
    }

    @Override
    protected MoveToStepRequest doParseInstance(XContentParser parser) {
        return MoveToStepRequest.parseRequest(index, parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected MoveToStepRequest mutateInstance(MoveToStepRequest request) {
        String index = request.getIndex();
        StepKey currentStepKey = request.getCurrentStepKey();
        StepKey nextStepKey = request.getNextStepKey();

        switch (between(0, 2)) {
            case 0:
                index += randomAlphaOfLength(5);
                break;
            case 1:
                currentStepKey = stepKeyTests.mutateInstance(currentStepKey);
                break;
            case 2:
                nextStepKey = stepKeyTests.mutateInstance(nextStepKey);
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }

        return new MoveToStepRequest(index, currentStepKey, nextStepKey);
    }

    public void testNullIndex() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> new MoveToStepRequest(null, stepKeyTests.createTestInstance(), stepKeyTests.createTestInstance()));
        assertEquals("index cannot be null", exception.getMessage());
    }
    
    public void testNullNextStep() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> new MoveToStepRequest(randomAlphaOfLength(10), stepKeyTests.createTestInstance(), null));
        assertEquals("next_step cannot be null", exception.getMessage());
    }
    
    public void testNullCurrentStep() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> new MoveToStepRequest(randomAlphaOfLength(10), null, stepKeyTests.createTestInstance()));
        assertEquals("current_step cannot be null", exception.getMessage());
    }
}
