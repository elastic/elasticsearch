/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.ESTestCase;

import java.util.function.Supplier;

import static org.elasticsearch.xpack.monitoring.exporter.http.AsyncHttpResourceHelper.mockBooleanActionListener;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link HttpResource}.
 */
public class HttpResourceTests extends ESTestCase {

    private final String owner = getTestName();
    private final RestClient client = mock(RestClient.class);

    public void testConstructorRequiresOwner() {
        expectThrows(NullPointerException.class, () -> new HttpResource(null) {
            @Override
            protected void doCheckAndPublish(RestClient client, ActionListener<Boolean> listener) {
                listener.onResponse(false);
            }
        });
    }

    public void testConstructor() {
        final HttpResource resource = new HttpResource(owner) {
            @Override
            protected void doCheckAndPublish(RestClient client, ActionListener<Boolean> listener) {
                listener.onResponse(false);
            }
        };

        assertSame(owner, resource.resourceOwnerName);
        assertTrue(resource.isDirty());
    }

    public void testConstructorDirtiness() {
        final boolean dirty = randomBoolean();
        final HttpResource resource = new HttpResource(owner, dirty) {
            @Override
            protected void doCheckAndPublish(RestClient client, ActionListener<Boolean> listener) {
                listener.onResponse(false);
            }
        };

        assertSame(owner, resource.resourceOwnerName);
        assertEquals(dirty, resource.isDirty());
    }

    public void testDirtiness() {
        final ActionListener<Boolean> listener = mockBooleanActionListener();
        // MockHttpResponse always succeeds for checkAndPublish
        final HttpResource resource = new MockHttpResource(owner);

        assertTrue(resource.isDirty());

        resource.markDirty();

        assertTrue(resource.isDirty());

        // if this fails, then the mocked resource needs to be fixed
        resource.checkAndPublish(client, listener);

        verify(listener).onResponse(true);
        assertFalse(resource.isDirty());
    }

    public void testCheckAndPublish() {
        final ActionListener<Boolean> listener = mockBooleanActionListener();
        final boolean expected = randomBoolean();
        // the default dirtiness should be irrelevant; it should always be run!
        final HttpResource resource = new HttpResource(owner) {
            @Override
            protected void doCheckAndPublish(RestClient client, ActionListener<Boolean> listener) {
                listener.onResponse(expected);
            }
        };

        resource.checkAndPublish(client, listener);

        verify(listener).onResponse(expected);
    }

    public void testCheckAndPublishEvenWhenDirty() {
        final ActionListener<Boolean> listener1 = mockBooleanActionListener();
        final ActionListener<Boolean> listener2 = mockBooleanActionListener();
        @SuppressWarnings("unchecked")
        final Supplier<Boolean> supplier = mock(Supplier.class);
        when(supplier.get()).thenReturn(true, false);

        final HttpResource resource = new HttpResource(owner) {
            @Override
            protected void doCheckAndPublish(RestClient client, ActionListener<Boolean> listener) {
                listener.onResponse(supplier.get());
            }
        };

        assertTrue(resource.isDirty());
        resource.checkAndPublish(client, listener1);
        verify(listener1).onResponse(true);
        assertFalse(resource.isDirty());
        resource.checkAndPublish(client, listener2);
        verify(listener2).onResponse(false);

        verify(supplier, times(2)).get();
    }

    public void testCheckAndPublishIfDirtyFalseWhileChecking() throws InterruptedException {
        final CountDownLatch firstCheck = new CountDownLatch(1);
        final CountDownLatch secondCheck = new CountDownLatch(1);

        final boolean response = randomBoolean();
        final ActionListener<Boolean> listener = mockBooleanActionListener();
        // listener used while checking is blocked, and thus should be ignored
        final ActionListener<Boolean> checkingListener = ActionListener.wrap(
            success -> {
                // busy checking, so this should be ignored
                assertFalse(success);
                secondCheck.countDown();
            },
            e -> {
                fail(e.getMessage());
                secondCheck.countDown();
            }
        );

        // the default dirtiness should be irrelevant; it should always be run!
        final HttpResource resource = new HttpResource(owner) {
            @Override
            protected void doCheckAndPublish(RestClient client, ActionListener<Boolean> listener) {
                // wait until the second check has had a chance to run to completion,
                // then respond here
                final Thread thread = new Thread(() -> {
                    try {
                        assertTrue(secondCheck.await(15, TimeUnit.SECONDS));
                        listener.onResponse(response);
                    } catch (InterruptedException e) {
                        listener.onFailure(e);
                    }

                    firstCheck.countDown();
                });
                thread.start();
            }
        };

        resource.checkAndPublishIfDirty(client, listener);
        resource.checkAndPublishIfDirty(client, checkingListener);

        assertTrue(firstCheck.await(15, TimeUnit.SECONDS));

        verify(listener).onResponse(response);

    }

    public void testCheckAndPublishIfDirty() {
        final ActionListener<Boolean> listener1 = mockBooleanActionListener();
        final ActionListener<Boolean> listener2 = mockBooleanActionListener();
        @SuppressWarnings("unchecked")
        final Supplier<Boolean> supplier = mock(Supplier.class);
        when(supplier.get()).thenReturn(true, false);

        final HttpResource resource = new HttpResource(owner) {
            @Override
            protected void doCheckAndPublish(RestClient client, ActionListener<Boolean> listener) {
                listener.onResponse(supplier.get());
            }
        };

        assertTrue(resource.isDirty());
        resource.checkAndPublishIfDirty(client, listener1);
        verify(listener1).onResponse(true);
        assertFalse(resource.isDirty());
        resource.checkAndPublishIfDirty(client, listener2);
        verify(listener2).onResponse(true);

        // once is the default!
        verify(supplier).get();
    }

}
