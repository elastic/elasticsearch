/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpHost;
import org.apache.lucene.util.SetOnce.AlreadySetException;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests {@link NodeFailureListener}.
 */
public class NodeFailureListenerTests extends ESTestCase {

    private final Sniffer sniffer = mock(Sniffer.class);
    private final HttpResource resource = new MockHttpResource(getTestName(), false);
    private final Node node = new Node(new HttpHost("localhost", 9200));

    private final NodeFailureListener listener = new NodeFailureListener();

    public void testSetSnifferTwiceFails() {
        listener.setSniffer(sniffer);

        assertThat(listener.getSniffer(), is(sniffer));

        expectThrows(AlreadySetException.class, () -> listener.setSniffer(randomFrom(sniffer, null)));
    }

    public void testSetResourceTwiceFails() {
        listener.setResource(resource);

        assertThat(listener.getResource(), is(resource));

        expectThrows(AlreadySetException.class, () -> listener.setResource(randomFrom(resource, null)));
    }

    public void testSnifferNotifiedOnFailure() {
        listener.setSniffer(sniffer);

        listener.onFailure(node);

        verify(sniffer).sniffOnFailure();
    }

    public void testResourceNotifiedOnFailure() {
        listener.setResource(resource);

        listener.onFailure(node);

        assertTrue(resource.isDirty());
    }

    public void testResourceAndSnifferNotifiedOnFailure() {
        final HttpResource optionalResource = randomFrom(resource, null);
        final Sniffer optionalSniffer = randomFrom(sniffer, null);

        listener.setResource(optionalResource);
        listener.setSniffer(optionalSniffer);

        listener.onFailure(node);

        if (optionalResource != null) {
            assertTrue(resource.isDirty());
        }

        if (optionalSniffer != null) {
            verify(sniffer).sniffOnFailure();
        }
    }

}
