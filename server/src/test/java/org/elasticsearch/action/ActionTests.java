/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.test.ESTestCase;

public class ActionTests extends ESTestCase {

    public void testEquals() {
        class FakeAction extends ActionType<ActionResponse> {
            protected FakeAction(String name) {
                super(name, null);
            }
        }
        FakeAction fakeAction1 = new FakeAction("a");
        FakeAction fakeAction2 = new FakeAction("a");
        FakeAction fakeAction3 = new FakeAction("b");
        String s = "Some random other object";
        assertEquals(fakeAction1, fakeAction1);
        assertEquals(fakeAction2, fakeAction2);
        assertNotEquals(fakeAction1, null);
        assertNotEquals(fakeAction1, fakeAction3);
        assertNotEquals(fakeAction1, s);
    }
}
