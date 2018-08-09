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


import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

public class StepKeyTests extends AbstractSerializingTestCase<StepKey> {

    @Override
    public StepKey createTestInstance() {
        return new StepKey(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    @Override
    protected Writeable.Reader<StepKey> instanceReader() {
        return StepKey::new;
    }

    @Override
    protected StepKey doParseInstance(XContentParser parser) {
        return StepKey.parse(parser);
    }

    @Override
    public StepKey mutateInstance(StepKey instance) {
        String phase = instance.getPhase();
        String action = instance.getAction();
        String step = instance.getName();

        switch (between(0, 2)) {
        case 0:
            phase += randomAlphaOfLength(5);
            break;
        case 1:
            action += randomAlphaOfLength(5);
            break;
        case 2:
            step += randomAlphaOfLength(5);
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }

        return new StepKey(phase, action, step);
    }
}
