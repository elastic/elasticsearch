/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.monitoring.exporter.http.AsyncHttpResourceHelper.mockBooleanActionListener;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests {@link MultiHttpResource}.
 */
public class MultiHttpResourceTests extends ESTestCase {

    private final String owner = getClass().getSimpleName();
    private final RestClient client = mock(RestClient.class);
    private final ActionListener<Boolean> listener = mockBooleanActionListener();

    public void testDoCheckAndPublish() {
        final List<MockHttpResource> allResources = successfulResources();
        final MultiHttpResource multiResource = new MultiHttpResource(owner, allResources);

        multiResource.doCheckAndPublish(client, listener);

        verify(listener).onResponse(true);

        for (final MockHttpResource resource : allResources) {
            assertSuccessfulResource(resource);
        }
    }

    public void testDoCheckAndPublishShortCircuits() {
        // fail either the check or the publish
        final Boolean check = randomBoolean() ? null : false;
        final boolean publish = check == null;
        final List<MockHttpResource> allResources = successfulResources();
        final MockHttpResource failureResource = new MockHttpResource(owner, true, check, publish);

        allResources.add(failureResource);

        Collections.shuffle(allResources, random());

        final MultiHttpResource multiResource = new MultiHttpResource(owner, allResources);

        multiResource.doCheckAndPublish(client, listener);

        if (check == null) {
            verify(listener).onFailure(any(Exception.class));
        } else {
            verify(listener).onResponse(false);
        }

        boolean found = false;

        for (final MockHttpResource resource : allResources) {
            // should stop looking at this point
            if (resource == failureResource) {
                assertThat(resource.checked, equalTo(1));
                if (resource.check == null) {
                    assertThat(resource.published, equalTo(0));
                } else {
                    assertThat(resource.published, equalTo(1));
                }

                found = true;
            } else if (found) {
                assertThat(resource.checked, equalTo(0));
                assertThat(resource.published, equalTo(0));
            }
            else {
                assertSuccessfulResource(resource);
            }
        }
    }

    public void testGetResources() {
        final List<MockHttpResource> allResources = successfulResources();
        final MultiHttpResource multiResource = new MultiHttpResource(owner, allResources);

        assertThat(multiResource.getResources(), equalTo(allResources));
    }

    private List<MockHttpResource> successfulResources() {
        final int successful = randomIntBetween(2, 5);
        final List<MockHttpResource> resources = new ArrayList<>(successful);

        for (int i = 0; i < successful; ++i) {
            final boolean check = randomBoolean();
            final MockHttpResource resource = new MockHttpResource(owner, randomBoolean(), check, check == false);

            resources.add(resource);
        }

        return resources;
    }

    private void assertSuccessfulResource(final MockHttpResource resource) {
        assertThat(resource.checked, equalTo(1));
        if (resource.check == false) {
            assertThat(resource.published, equalTo(1));
        } else {
            assertThat(resource.published, equalTo(0));
        }
    }

}
