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
package org.elasticsearch.client.ilm;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class SetPriorityActionTests extends AbstractXContentTestCase<SetPriorityAction> {

    @Override
    protected SetPriorityAction doParseInstance(XContentParser parser) throws IOException {
        return SetPriorityAction.parse(parser);
    }

    @Override
    protected SetPriorityAction createTestInstance() {
        return randomInstance();
    }

    static SetPriorityAction randomInstance() {
        return new SetPriorityAction(randomIntBetween(1, 100));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public void testNonPositivePriority() {
        Exception e = expectThrows(Exception.class, () -> new SetPriorityAction(randomIntBetween(-100, -1)));
        assertThat(e.getMessage(), equalTo("[priority] must be 0 or greater"));
    }

    public void testNullPriorityAllowed(){
        SetPriorityAction nullPriority = new SetPriorityAction(null);
        assertNull(nullPriority.recoveryPriority);
    }

    public void testEqualsAndHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createTestInstance(), this::copy);
    }

    SetPriorityAction copy(SetPriorityAction setPriorityAction) {
        return new SetPriorityAction(setPriorityAction.recoveryPriority);
    }

    SetPriorityAction notCopy(SetPriorityAction setPriorityAction) {
        return new SetPriorityAction(setPriorityAction.recoveryPriority + 1);
    }
}
