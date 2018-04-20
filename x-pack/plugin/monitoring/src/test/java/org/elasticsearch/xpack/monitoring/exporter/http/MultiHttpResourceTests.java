/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.monitoring.exporter.http.PublishableHttpResource.CheckResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link MultiHttpResource}.
 */
public class MultiHttpResourceTests extends ESTestCase {

    private final String owner = getClass().getSimpleName();
    private final RestClient client = mock(RestClient.class);

    public void testDoCheckAndPublish() {
        final List<MockHttpResource> allResources = successfulResources();
        final MultiHttpResource multiResource = new MultiHttpResource(owner, allResources);

        assertTrue(multiResource.doCheckAndPublish(client));

        for (final MockHttpResource resource : allResources) {
            assertSuccessfulResource(resource);
        }
    }

    public void testDoCheckAndPublishShortCircuits() {
        // fail either the check or the publish
        final CheckResponse check = randomFrom(CheckResponse.ERROR, CheckResponse.DOES_NOT_EXIST);
        final boolean publish = check == CheckResponse.ERROR;
        final List<MockHttpResource> allResources = successfulResources();
        final MockHttpResource failureResource = new MockHttpResource(owner, true, check, publish);

        allResources.add(failureResource);

        Collections.shuffle(allResources, random());

        final MultiHttpResource multiResource = new MultiHttpResource(owner, allResources);

        assertFalse(multiResource.doCheckAndPublish(client));

        boolean found = false;

        for (final MockHttpResource resource : allResources) {
            // should stop looking at this point
            if (resource == failureResource) {
                assertThat(resource.checked, equalTo(1));
                if (resource.check == CheckResponse.ERROR) {
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
            final CheckResponse check = randomFrom(CheckResponse.DOES_NOT_EXIST, CheckResponse.EXISTS);
            final MockHttpResource resource = new MockHttpResource(owner, randomBoolean(), check, check == CheckResponse.DOES_NOT_EXIST);

            resources.add(resource);
        }

        return resources;
    }

    private void assertSuccessfulResource(final MockHttpResource resource) {
        assertThat(resource.checked, equalTo(1));
        if (resource.check == CheckResponse.DOES_NOT_EXIST) {
            assertThat(resource.published, equalTo(1));
        } else {
            assertThat(resource.published, equalTo(0));
        }
    }

}
