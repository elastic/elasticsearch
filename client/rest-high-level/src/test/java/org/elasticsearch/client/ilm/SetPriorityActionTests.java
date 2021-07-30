/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
