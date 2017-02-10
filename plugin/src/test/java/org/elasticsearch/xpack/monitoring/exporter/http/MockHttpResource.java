/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Collections;
import java.util.Map;

/**
 * {@code MockHttpResource} the {@linkplain HttpResource#isDirty() dirtiness} to be defaulted.
 */
public class MockHttpResource extends PublishableHttpResource {

    public final CheckResponse check;
    public final boolean publish;

    public int checked = 0;
    public int published = 0;

    /**
     * Create a new {@link MockHttpResource} that starts dirty, but always succeeds.
     *
     * @param resourceOwnerName The user-recognizable name
     */
    public MockHttpResource(final String resourceOwnerName) {
        this(resourceOwnerName, true, CheckResponse.EXISTS, true);
    }

    /**
     * Create a new {@link MockHttpResource} that starts {@code dirty}, but always succeeds.
     *
     * @param resourceOwnerName The user-recognizable name
     * @param dirty The starting dirtiness of the resource.
     */
    public MockHttpResource(final String resourceOwnerName, final boolean dirty) {
        this(resourceOwnerName, dirty, CheckResponse.EXISTS, true);
    }

    /**
     * Create a new {@link MockHttpResource} that starts dirty, but always succeeds.
     *
     * @param resourceOwnerName The user-recognizable name.
     * @param masterTimeout Master timeout to use with any request.
     * @param parameters The base parameters to specify for the request.
     */
    public MockHttpResource(final String resourceOwnerName, @Nullable final TimeValue masterTimeout, final Map<String, String> parameters) {
        this(resourceOwnerName, masterTimeout, parameters, true, CheckResponse.EXISTS, true);
    }

    /**
     * Create a new {@link MockHttpResource} that starts {@code dirty}.
     *
     * @param resourceOwnerName The user-recognizable name
     * @param dirty The starting dirtiness of the resource.
     * @param check The expected response when checking for the resource.
     * @param publish The expected response when publishing the resource (assumes check was {@link CheckResponse#DOES_NOT_EXIST}).
     */
    public MockHttpResource(final String resourceOwnerName, final boolean dirty, final CheckResponse check, final boolean publish) {
        this(resourceOwnerName, null, Collections.emptyMap(), dirty, check, publish);
    }

    /**
     * Create a new {@link MockHttpResource} that starts dirty.
     *
     * @param resourceOwnerName The user-recognizable name
     * @param check The expected response when checking for the resource.
     * @param publish The expected response when publishing the resource (assumes check was {@link CheckResponse#DOES_NOT_EXIST}).
     * @param masterTimeout Master timeout to use with any request.
     * @param parameters The base parameters to specify for the request.
     */
    public MockHttpResource(final String resourceOwnerName, @Nullable final TimeValue masterTimeout, final Map<String, String> parameters,
                            final CheckResponse check, final boolean publish) {
        this(resourceOwnerName, masterTimeout, parameters, true, check, publish);
    }

    /**
     * Create a new {@link MockHttpResource}.
     *
     * @param resourceOwnerName The user-recognizable name
     * @param dirty The starting dirtiness of the resource.
     * @param check The expected response when checking for the resource.
     * @param publish The expected response when publishing the resource (assumes check was {@link CheckResponse#DOES_NOT_EXIST}).
     * @param masterTimeout Master timeout to use with any request.
     * @param parameters The base parameters to specify for the request.
     */
    public MockHttpResource(final String resourceOwnerName, @Nullable final TimeValue masterTimeout, final Map<String, String> parameters,
                            final boolean dirty, final CheckResponse check, final boolean publish) {
        super(resourceOwnerName, masterTimeout, parameters, dirty);

        this.check = check;
        this.publish = publish;
    }

    @Override
    protected CheckResponse doCheck(final RestClient client) {
        assert client != null;

        ++checked;

        return check;
    }

    @Override
    protected boolean doPublish(final RestClient client) {
        assert client != null;

        ++published;

        return publish;
    }

}
