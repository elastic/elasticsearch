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
package org.elasticsearch.protocol.xpack.ml.job.config;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

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
