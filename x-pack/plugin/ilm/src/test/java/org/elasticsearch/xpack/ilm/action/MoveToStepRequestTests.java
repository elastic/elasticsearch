/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 */
package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.ilm.StepKeyTests;
import org.junit.Before;

public class MoveToStepRequestTests extends AbstractXContentSerializingTestCase<TransportMoveToStepAction.Request> {

    private String index;
    private static final StepKeyTests stepKeyTests = new StepKeyTests();

    @Before
    public void setup() {
        index = randomAlphaOfLength(5);
    }

    @Override
    protected TransportMoveToStepAction.Request createTestInstance() {
        return new TransportMoveToStepAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            index,
            stepKeyTests.createTestInstance(),
            randomStepSpecification()
        );
    }

    @Override
    protected Writeable.Reader<TransportMoveToStepAction.Request> instanceReader() {
        return TransportMoveToStepAction.Request::new;
    }

    @Override
    protected TransportMoveToStepAction.Request doParseInstance(XContentParser parser) {
        return TransportMoveToStepAction.Request.parseRequest(
            (currentStepKey, nextStepKey) -> new TransportMoveToStepAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                index,
                currentStepKey,
                nextStepKey
            ),
            parser
        );
    }

    @Override
    protected TransportMoveToStepAction.Request mutateInstance(TransportMoveToStepAction.Request request) {
        String indexName = request.getIndex();
        StepKey currentStepKey = request.getCurrentStepKey();
        TransportMoveToStepAction.Request.PartialStepKey nextStepKey = request.getNextStepKey();

        switch (between(0, 2)) {
            case 0 -> indexName += randomAlphaOfLength(5);
            case 1 -> currentStepKey = stepKeyTests.mutateInstance(currentStepKey);
            case 2 -> nextStepKey = randomValueOtherThan(nextStepKey, MoveToStepRequestTests::randomStepSpecification);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new TransportMoveToStepAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, indexName, currentStepKey, nextStepKey);
    }

    private static TransportMoveToStepAction.Request.PartialStepKey randomStepSpecification() {
        if (randomBoolean()) {
            StepKey key = stepKeyTests.createTestInstance();
            return new TransportMoveToStepAction.Request.PartialStepKey(key.phase(), key.action(), key.name());
        } else {
            String phase = randomAlphaOfLength(10);
            String action = randomBoolean() ? null : randomAlphaOfLength(6);
            String name = action == null ? null : (randomBoolean() ? null : randomAlphaOfLength(6));
            return new TransportMoveToStepAction.Request.PartialStepKey(phase, action, name);
        }
    }
}
