/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * {@code VersionHttpResource} verifies that the returned {@link Version} of Elasticsearch is at least the specified minimum version.
 */
public class VersionHttpResource extends HttpResource {

    private static final Logger logger = LogManager.getLogger(VersionHttpResource.class);

    /**
     * The minimum supported version of Elasticsearch.
     */
    private final Version minimumVersion;

    /**
     * Create a new {@link VersionHttpResource}.
     *
     * @param resourceOwnerName The user-recognizable name.
     * @param minimumVersion The minimum supported version of Elasticsearch.
     */
    public VersionHttpResource(final String resourceOwnerName, final Version minimumVersion) {
        super(resourceOwnerName);

        this.minimumVersion = Objects.requireNonNull(minimumVersion);
    }

    /**
     * Verify that the minimum {@link Version} is supported on the remote cluster.
     * <p>
     * If it does not, then there is nothing that can be done except wait until it does. There is no publishing aspect to this operation.
     */
    @Override
    protected void doCheckAndPublish(final RestClient client, final ActionListener<ResourcePublishResult> listener) {
        logger.trace("checking [{}] to ensure that it supports the minimum version [{}]", resourceOwnerName, minimumVersion);

        final Request request = new Request("GET", "/");
        request.addParameter("filter_path", "version.number");

        client.performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(final Response response) {
                try {
                    // malformed responses can cause exceptions during validation
                    listener.onResponse(validateVersion(response));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(final Exception exception) {
                logger.error((Supplier<?>) () ->
                             new ParameterizedMessage("failed to verify minimum version [{}] on the [{}] monitoring cluster",
                                                      minimumVersion, resourceOwnerName),
                             exception);

                listener.onFailure(exception);
            }
        });
    }

    /**
     * Ensure that the {@code response} contains a {@link Version} that is {@linkplain Version#onOrAfter(Version) on or after} the
     * {@link #minimumVersion}.
     *
     * @param response The response to parse.
     * @return A ready result if the remote cluster is running a supported version.
     * @throws NullPointerException if the response is malformed.
     * @throws ClassCastException if the response is malformed.
     * @throws IOException if any parsing issue occurs.
     */
    private ResourcePublishResult validateVersion(final Response response) throws IOException {
        Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), false);
        // the response should be filtered to just '{"version":{"number":"xyz"}}', so this is cheap and guaranteed
        @SuppressWarnings("unchecked")
        final String versionNumber = (String) ((Map<String, Object>) map.get("version")).get("number");
        final Version version = Version.fromString(
            versionNumber
                .replace("-SNAPSHOT", "")
                .replaceFirst("-(alpha\\d+|beta\\d+|rc\\d+)", "")
        );

        if (version.onOrAfter(minimumVersion)) {
            logger.debug("version [{}] >= [{}] and supported for [{}]", version, minimumVersion, resourceOwnerName);
            return ResourcePublishResult.ready();
        } else {
            logger.error("version [{}] < [{}] and NOT supported for [{}]", version, minimumVersion, resourceOwnerName);
            return ResourcePublishResult.notReady("version [" + version + "] < [" + minimumVersion + "] and NOT supported for ["
                + resourceOwnerName + "]");
        }
    }

}
