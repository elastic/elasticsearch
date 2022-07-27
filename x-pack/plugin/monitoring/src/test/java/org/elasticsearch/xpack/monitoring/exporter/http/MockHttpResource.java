/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.util.Collections;
import java.util.Map;

/**
 * {@code MockHttpResource} the {@linkplain HttpResource#isDirty() dirtiness} to be defaulted.
 */
public class MockHttpResource extends PublishableHttpResource {

    public final Boolean check;
    public final Boolean publish;

    public int checked = 0;
    public int published = 0;

    /**
     * Create a new {@link MockHttpResource} that starts dirty, but always succeeds.
     *
     * @param resourceOwnerName The user-recognizable name
     */
    public MockHttpResource(final String resourceOwnerName) {
        this(resourceOwnerName, true, true, true);
    }

    /**
     * Create a new {@link MockHttpResource} that starts {@code dirty}, but always succeeds.
     *
     * @param resourceOwnerName The user-recognizable name
     * @param dirty The starting dirtiness of the resource.
     */
    public MockHttpResource(final String resourceOwnerName, final boolean dirty) {
        this(resourceOwnerName, dirty, true, true);
    }

    /**
     * Create a new {@link MockHttpResource} that starts dirty, but always succeeds.
     *
     * @param resourceOwnerName The user-recognizable name.
     * @param masterTimeout Master timeout to use with any request.
     * @param parameters The base parameters to specify for the request.
     */
    public MockHttpResource(final String resourceOwnerName, @Nullable final TimeValue masterTimeout, final Map<String, String> parameters) {
        this(resourceOwnerName, masterTimeout, parameters, true, true, true);
    }

    /**
     * Create a new {@link MockHttpResource} that starts {@code dirty}.
     *
     * @param resourceOwnerName The user-recognizable name
     * @param dirty The starting dirtiness of the resource.
     * @param check The expected response when checking for the resource.
     * @param publish The expected response when publishing the resource (assumes check was {@code false}).
     */
    public MockHttpResource(final String resourceOwnerName, final boolean dirty, final Boolean check, final Boolean publish) {
        this(resourceOwnerName, null, Collections.emptyMap(), dirty, check, publish);
    }

    /**
     * Create a new {@link MockHttpResource} that starts dirty.
     *
     * @param resourceOwnerName The user-recognizable name
     * @param check The expected response when checking for the resource.
     * @param publish The expected response when publishing the resource (assumes check was {@code false}).
     * @param masterTimeout Master timeout to use with any request.
     * @param parameters The base parameters to specify for the request.
     */
    public MockHttpResource(
        final String resourceOwnerName,
        @Nullable final TimeValue masterTimeout,
        final Map<String, String> parameters,
        final Boolean check,
        final Boolean publish
    ) {
        this(resourceOwnerName, masterTimeout, parameters, true, check, publish);
    }

    /**
     * Create a new {@link MockHttpResource}.
     *
     * @param resourceOwnerName The user-recognizable name
     * @param dirty The starting dirtiness of the resource.
     * @param check The expected response when checking for the resource.
     * @param publish The expected response when publishing the resource (assumes check was {@code false}).
     * @param masterTimeout Master timeout to use with any request.
     * @param parameters The base parameters to specify for the request.
     */
    public MockHttpResource(
        final String resourceOwnerName,
        @Nullable final TimeValue masterTimeout,
        final Map<String, String> parameters,
        final boolean dirty,
        final Boolean check,
        final Boolean publish
    ) {
        super(resourceOwnerName, masterTimeout, parameters, dirty);

        this.check = check;
        this.publish = publish;
    }

    @Override
    protected void doCheck(final RestClient client, final ActionListener<Boolean> listener) {
        assert client != null;

        ++checked;

        if (check == null) {
            listener.onFailure(new RuntimeException("TEST - expected"));
        } else {
            listener.onResponse(check);
        }
    }

    @Override
    protected void doPublish(final RestClient client, final ActionListener<ResourcePublishResult> listener) {
        assert client != null;

        ++published;

        if (publish == null) {
            listener.onFailure(new RuntimeException("TEST - expected"));
        } else {
            listener.onResponse(new ResourcePublishResult(publish));
        }
    }

}
