/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.test.AbstractXContentSerializingTestCase;

public abstract class AbstractActionTestCase<T extends LifecycleAction> extends AbstractXContentSerializingTestCase<T> {

    public abstract void testToSteps();

    protected boolean isSafeAction() {
        return true;
    }

    public final void testIsSafeAction() {
        LifecycleAction action = createTestInstance();
        assertEquals(isSafeAction(), action.isSafeAction());
    }

}
