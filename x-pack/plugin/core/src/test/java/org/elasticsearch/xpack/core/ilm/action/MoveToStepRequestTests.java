/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 */
package org.elasticsearch.xpack.core.ilm.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.ilm.StepKeyTests;
import org.elasticsearch.xpack.core.ilm.action.MoveToStepAction.Request;
import org.junit.Before;

public class MoveToStepRequestTests extends AbstractXContentSerializingTestCase<Request> {

    private String index;
    private static final StepKeyTests stepKeyTests = new StepKeyTests();

    @Before
    public void setup() {
        index = randomAlphaOfLength(5);
    }

    @Override
    protected Request createTestInstance() {
        return new Request(index, stepKeyTests.createTestInstance(), randomStepSpecification());
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return Request.parseRequest(index, parser);
    }

    @Override
    protected Request mutateInstance(Request request) {
        String indexName = request.getIndex();
        StepKey currentStepKey = request.getCurrentStepKey();
        Request.PartialStepKey nextStepKey = request.getNextStepKey();

        switch (between(0, 2)) {
            case 0 -> indexName += randomAlphaOfLength(5);
            case 1 -> currentStepKey = stepKeyTests.mutateInstance(currentStepKey);
            case 2 -> nextStepKey = randomValueOtherThan(nextStepKey, MoveToStepRequestTests::randomStepSpecification);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new Request(indexName, currentStepKey, nextStepKey);
    }

    private static Request.PartialStepKey randomStepSpecification() {
        if (randomBoolean()) {
            StepKey key = stepKeyTests.createTestInstance();
            return new Request.PartialStepKey(key.phase(), key.action(), key.name());
        } else {
            String phase = randomAlphaOfLength(10);
            String action = randomBoolean() ? null : randomAlphaOfLength(6);
            String name = action == null ? null : (randomBoolean() ? null : randomAlphaOfLength(6));
            return new Request.PartialStepKey(phase, action, name);
        }
    }
}
