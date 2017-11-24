/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class MockActionTests extends AbstractSerializingTestCase<MockAction> {

    @Override
    protected MockAction createTestInstance() {
        return new MockAction(randomBoolean() ? null : randomBoolean(), randomLong());
    }

    @Override
    protected MockAction doParseInstance(XContentParser parser) throws IOException {
        return MockAction.parse(parser);
    }

    @Override
    protected Reader<MockAction> instanceReader() {
        return MockAction::new;
    }

    @Override
    protected MockAction mutateInstance(MockAction instance) throws IOException {
        boolean completed = instance.wasCompleted();
        long executedCount = instance.getExecutedCount();
        switch (randomIntBetween(0, 1)) {
        case 0:
            completed = completed == false;
            break;
        case 1:
            executedCount = executedCount + randomInt(1000);
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new MockAction(completed, executedCount);
    }

    public void testExecuteNotComplete() {

        MockAction action = new MockAction();
        action.setCompleteOnExecute(false);

        assertFalse(action.wasCompleted());
        assertEquals(0L, action.getExecutedCount());

        SetOnce<Boolean> listenerCalled = new SetOnce<>();

        action.execute(null, null, new LifecycleAction.Listener() {

            @Override
            public void onSuccess(boolean completed) {
                listenerCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        });

        assertFalse(action.wasCompleted());
        assertEquals(1L, action.getExecutedCount());
        assertEquals(true, listenerCalled.get());
    }

    public void testExecuteComplete() {

        MockAction action = new MockAction();
        action.setCompleteOnExecute(true);

        assertFalse(action.wasCompleted());
        assertEquals(0L, action.getExecutedCount());

        SetOnce<Boolean> listenerCalled = new SetOnce<>();

        action.execute(null, null, new LifecycleAction.Listener() {

            @Override
            public void onSuccess(boolean completed) {
                listenerCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        });

        assertTrue(action.wasCompleted());
        assertEquals(1L, action.getExecutedCount());
        assertEquals(true, listenerCalled.get());
    }

    public void testExecuteFailure() {
        Exception exception = new RuntimeException();

        MockAction action = new MockAction();
        action.setCompleteOnExecute(randomBoolean());
        action.setExceptionToThrow(exception);

        assertFalse(action.wasCompleted());
        assertEquals(0L, action.getExecutedCount());

        SetOnce<Boolean> listenerCalled = new SetOnce<>();

        action.execute(null, null, new LifecycleAction.Listener() {

            @Override
            public void onSuccess(boolean completed) {
                throw new AssertionError("Unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                assertSame(exception, e);
                listenerCalled.set(true);
            }
        });

        assertFalse(action.wasCompleted());
        assertEquals(1L, action.getExecutedCount());
        assertEquals(true, listenerCalled.get());
    }

}
