/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpEntity;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * {@code PublishableHttpResource} represents an {@link HttpResource} that is a single file or object that can be checked <em>and</em>
 * published in the event that the check does not pass.
 *
 * @see #doCheck(RestClient)
 * @see #doPublish(RestClient)
 */
public abstract class PublishableHttpResource extends HttpResource {

    /**
     * {@code CheckResponse} provides a ternary state for {@link #doCheck(RestClient)}.
     */
    public enum CheckResponse {

        /**
         * The check found the resource, so nothing needs to be published.
         */
        EXISTS,
        /**
         * The check did not find the resource, so we need to attempt to publish it.
         */
        DOES_NOT_EXIST,
        /**
         * The check hit an unexpected exception that should block publishing attempts until it can check again.
         */
        ERROR

    }

    /**
     * A value that will never match anything in the JSON response body, thus limiting it to "{}".
     */
    public static final String FILTER_PATH_NONE = "$NONE";

    /**
     * Use this to avoid getting any JSON response from a request.
     */
    public static final Map<String, String> NO_BODY_PARAMETERS = Collections.singletonMap("filter_path", FILTER_PATH_NONE);

    /**
     * The default parameters to use for any request.
     */
    protected final Map<String, String> parameters;

    /**
     * Create a new {@link PublishableHttpResource} that {@linkplain #isDirty() is dirty}.
     *
     * @param resourceOwnerName The user-recognizable name.
     * @param masterTimeout Master timeout to use with any request.
     * @param baseParameters The base parameters to specify for the request.
     */
    protected PublishableHttpResource(final String resourceOwnerName, @Nullable final TimeValue masterTimeout,
                                      final Map<String, String> baseParameters) {
        this(resourceOwnerName, masterTimeout, baseParameters, true);
    }

    /**
     * Create a new {@link PublishableHttpResource}.
     *
     * @param resourceOwnerName The user-recognizable name.
     * @param masterTimeout Master timeout to use with any request.
     * @param baseParameters The base parameters to specify for the request.
     * @param dirty Whether the resource is dirty or not
     */
    protected PublishableHttpResource(final String resourceOwnerName, @Nullable final TimeValue masterTimeout,
                                      final Map<String, String> baseParameters, final boolean dirty) {
        super(resourceOwnerName, dirty);

        if (masterTimeout != null) {
            final Map<String, String> parameters = new HashMap<>(baseParameters.size() + 1);

            parameters.putAll(baseParameters);
            parameters.put("master_timeout", masterTimeout.toString());

            this.parameters = Collections.unmodifiableMap(parameters);
        } else {
            this.parameters = baseParameters;
        }
    }

    /**
     * Get the default parameters to use with every request.
     *
     * @return Never {@code null}.
     */
    public Map<String, String> getParameters() {
        return parameters;
    }

    /**
     * Perform whatever is necessary to check and publish this {@link PublishableHttpResource}.
     *
     * @param client The REST client to make the request(s).
     * @return {@code true} if the resource is available for use. {@code false} to stop.
     */
    @Override
    protected final boolean doCheckAndPublish(final RestClient client) {
        final CheckResponse check = doCheck(client);

        // errors cause a dead-stop
        return check != CheckResponse.ERROR && (check == CheckResponse.EXISTS || doPublish(client));
    }

    /**
     * Determine if the current resource exists.
     * <ul>
     * <li>
     *     {@link CheckResponse#EXISTS EXISTS} will <em>not</em> run {@link #doPublish(RestClient)} and mark this as <em>not</em> dirty.
     * </li>
     * <li>
     *     {@link CheckResponse#DOES_NOT_EXIST DOES_NOT_EXIST} will run {@link #doPublish(RestClient)}, which determines the dirtiness.
     * </li>
     * <li>{@link CheckResponse#ERROR ERROR} will <em>not</em> run {@link #doPublish(RestClient)} and mark this as dirty.</li>
     * </ul>
     *
     * @param client The REST client to make the request(s).
     * @return Never {@code null}.
     */
    protected abstract CheckResponse doCheck(RestClient client);

    /**
     * Determine if the current {@code resourceName} exists at the {@code resourceBasePath} endpoint.
     * <p>
     * This provides the base-level check for any resource that does not need to care about its response beyond existence (and likely does
     * not need to inspect its contents).
     *
     * @param client The REST client to make the request(s).
     * @param logger The logger to use for status messages.
     * @param resourceBasePath The base path/endpoint to check for the resource (e.g., "/_template").
     * @param resourceName The name of the resource (e.g., "template123").
     * @param resourceType The type of resource (e.g., "monitoring template").
     * @param resourceOwnerName The user-recognizeable resource owner.
     * @param resourceOwnerType The type of resource owner being dealt with (e.g., "monitoring cluster").
     * @return Never {@code null}.
     */
    protected CheckResponse simpleCheckForResource(final RestClient client, final Logger logger,
                                                   final String resourceBasePath,
                                                   final String resourceName, final String resourceType,
                                                   final String resourceOwnerName, final String resourceOwnerType) {
        return checkForResource(client, logger, resourceBasePath, resourceName, resourceType, resourceOwnerName, resourceOwnerType).v1();
    }

    /**
     * Determine if the current {@code resourceName} exists at the {@code resourceBasePath} endpoint.
     * <p>
     * This provides the base-level check for any resource that cares about existence and also its contents.
     *
     * @param client The REST client to make the request(s).
     * @param logger The logger to use for status messages.
     * @param resourceBasePath The base path/endpoint to check for the resource (e.g., "/_template").
     * @param resourceName The name of the resource (e.g., "template123").
     * @param resourceType The type of resource (e.g., "monitoring template").
     * @param resourceOwnerName The user-recognizeable resource owner.
     * @param resourceOwnerType The type of resource owner being dealt with (e.g., "monitoring cluster").
     * @return Never {@code null} pair containing the checked response and the returned response.
     *         The response will only ever be {@code null} if none was returned.
     * @see #simpleCheckForResource(RestClient, Logger, String, String, String, String, String)
     */
    protected Tuple<CheckResponse, Response> checkForResource(final RestClient client, final Logger logger,
                                                              final String resourceBasePath,
                                                              final String resourceName, final String resourceType,
                                                              final String resourceOwnerName, final String resourceOwnerType) {
        logger.trace("checking if {} [{}] exists on the [{}] {}", resourceType, resourceName, resourceOwnerName, resourceOwnerType);

        try {
            final Response response = client.performRequest("GET", resourceBasePath + "/" + resourceName, parameters);

            // we don't currently check for the content because we always expect it to be the same;
            // if we ever make a BWC change to any template (thus without renaming it), then we need to check the content!
            if (response.getStatusLine().getStatusCode() == RestStatus.OK.getStatus()) {
                logger.debug("{} [{}] found on the [{}] {}", resourceType, resourceName, resourceOwnerName, resourceOwnerType);

                return new Tuple<>(CheckResponse.EXISTS, response);
            } else {
                throw new ResponseException(response);
            }
        } catch (final ResponseException e) {
            final Response response = e.getResponse();
            final int statusCode = response.getStatusLine().getStatusCode();

            // 404
            if (statusCode == RestStatus.NOT_FOUND.getStatus()) {
                logger.debug("{} [{}] does not exist on the [{}] {}", resourceType, resourceName, resourceOwnerName, resourceOwnerType);

                return new Tuple<>(CheckResponse.DOES_NOT_EXIST, response);
            } else {
                logger.error((Supplier<?>) () ->
                        new ParameterizedMessage("failed to verify {} [{}] on the [{}] {} with status code [{}]",
                                                 resourceType, resourceName, resourceOwnerName, resourceOwnerType, statusCode),
                        e);

                // weirder failure than below; block responses just like other unexpected failures
                return new Tuple<>(CheckResponse.ERROR, response);
            }
        } catch (IOException | RuntimeException e) {
            logger.error((Supplier<?>) () ->
                    new ParameterizedMessage("failed to verify {} [{}] on the [{}] {}",
                                             resourceType, resourceName, resourceOwnerName, resourceOwnerType),
                    e);

            // do not attempt to publish the resource because we're in a broken state
            return new Tuple<>(CheckResponse.ERROR, null);
        }
    }

    /**
     * Publish the current resource.
     * <p>
     * This is only invoked if {@linkplain #doCheck(RestClient) the check} fails.
     *
     * @param client The REST client to make the request(s).
     * @return {@code true} if it exists.
     */
    protected abstract boolean doPublish(RestClient client);

    /**
     * Upload the {@code resourceName} to the {@code resourceBasePath} endpoint.
     *
     * @param client The REST client to make the request(s).
     * @param logger The logger to use for status messages.
     * @param resourceBasePath The base path/endpoint to check for the resource (e.g., "/_template").
     * @param resourceName The name of the resource (e.g., "template123").
     * @param body The {@link HttpEntity} that makes up the body of the request.
     * @param resourceType The type of resource (e.g., "monitoring template").
     * @param resourceOwnerName The user-recognizeable resource owner.
     * @param resourceOwnerType The type of resource owner being dealt with (e.g., "monitoring cluster").
     */
    protected boolean putResource(final RestClient client, final Logger logger,
                                  final String resourceBasePath,
                                  final String resourceName, final java.util.function.Supplier<HttpEntity> body,
                                  final String resourceType,
                                  final String resourceOwnerName, final String resourceOwnerType) {
        logger.trace("uploading {} [{}] to the [{}] {}", resourceType, resourceName, resourceOwnerName, resourceOwnerType);

        boolean success = false;

        try {
            final Response response = client.performRequest("PUT", resourceBasePath + "/" + resourceName, parameters, body.get());
            final int statusCode = response.getStatusLine().getStatusCode();

            // 200 or 201
            if (statusCode == RestStatus.OK.getStatus() || statusCode == RestStatus.CREATED.getStatus()) {
                logger.debug("{} [{}] uploaded to the [{}] {}", resourceType, resourceName, resourceOwnerName, resourceOwnerType);

                success = true;
            } else {
                throw new RuntimeException("[" + resourceBasePath + "/" + resourceName + "] responded with [" + statusCode + "]");
            }
        } catch (IOException | RuntimeException e) {
            logger.error((Supplier<?>) () ->
                    new ParameterizedMessage("failed to upload {} [{}] on the [{}] {}",
                                             resourceType, resourceName, resourceOwnerName, resourceOwnerType),
                    e);
        }

        return success;
    }

}
