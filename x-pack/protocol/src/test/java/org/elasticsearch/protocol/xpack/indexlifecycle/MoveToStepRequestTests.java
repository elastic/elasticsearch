/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 *
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
