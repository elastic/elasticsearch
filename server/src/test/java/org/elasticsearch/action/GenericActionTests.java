package org.elasticsearch.action;

import org.elasticsearch.test.ESTestCase;

public class GenericActionTests extends ESTestCase {

    public void testEquals() {
        class FakeAction extends GenericAction {
            protected FakeAction(String name) {
                super(name);
            }

            @Override
            public ActionResponse newResponse() {
                return null;
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
