/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.RuleAction;

import static org.hamcrest.Matchers.equalTo;

public class RuleActionTests extends ESTestCase {

    public void testForString() {
        assertEquals(RuleAction.SKIP_RESULT, RuleAction.fromString("skip_result"));
        assertEquals(RuleAction.SKIP_RESULT, RuleAction.fromString("SKIP_RESULT"));
        assertEquals(RuleAction.SKIP_MODEL_UPDATE, RuleAction.fromString("skip_model_update"));
        assertEquals(RuleAction.SKIP_MODEL_UPDATE, RuleAction.fromString("SKIP_MODEL_UPDATE"));
    }

    public void testToString() {
        assertEquals("skip_result", RuleAction.SKIP_RESULT.toString());
        assertEquals("skip_model_update", RuleAction.SKIP_MODEL_UPDATE.toString());
    }

    public void testReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(0);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(RuleAction.readFromStream(in), equalTo(RuleAction.SKIP_RESULT));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(RuleAction.readFromStream(in), equalTo(RuleAction.SKIP_MODEL_UPDATE));
            }
        }
    }
}
