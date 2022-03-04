/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpEntity;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContent;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@code PublishableHttpResource} represents an {@link HttpResource} that is a single file or object that can be checked <em>and</em>
 * published in the event that the check does not pass.
 *
 * @see #doCheck(RestClient, ActionListener)
 * @see #doPublish(RestClient, ActionListener)
 */
public abstract class PublishableHttpResource extends HttpResource {

    /**
     * A value that will never match anything in the JSON response body, thus limiting it to "{}".
     */
    public static final String FILTER_PATH_NONE = "$NONE";
    /**
     * A value that will match any top-level key and an inner "version" field, like '{"any-key":{"version":123}}'.
     */
    public static final String FILTER_PATH_RESOURCE_VERSION = "*.version";

    /**
     * Use this to avoid getting any JSON response from a request.
     */
    public static final Map<String, String> NO_BODY_PARAMETERS = Collections.singletonMap("filter_path", FILTER_PATH_NONE);
    /**
     * Use this to retrieve the version of template and pipeline resources in their JSON response from a request.
     */
    public static final Map<String, String> RESOURCE_VERSION_PARAMETERS = Collections.singletonMap(
        "filter_path",
        FILTER_PATH_RESOURCE_VERSION
    );

    /**
     * The default set of acceptable exists response codes for GET requests.
     */
    public static final Set<Integer> GET_EXISTS = Collections.singleton(RestStatus.OK.getStatus());
    /**
     * The default set of <em>acceptable</em> response codes for GET requests to represent that it does NOT exist.
     */
    public static final Set<Integer> GET_DOES_NOT_EXIST = Collections.singleton(RestStatus.NOT_FOUND.getStatus());

    /**
     * The default parameters to use for any request.
     */
    protected final Map<String, String> defaultParameters;

    /**
     * Create a new {@link PublishableHttpResource} that {@linkplain #isDirty() is dirty}.
     *
     * @param resourceOwnerName The user-recognizable name.
     * @param masterTimeout Master timeout to use with any request.
     * @param baseParameters The base parameters to specify for the request.
     */
    protected PublishableHttpResource(
        final String resourceOwnerName,
        @Nullable final TimeValue masterTimeout,
        final Map<String, String> baseParameters
    ) {
        this(resourceOwnerName, masterTimeout, baseParameters, true);
    }

    /**
     * Create a new {@link PublishableHttpResource}.
     *
     * @param resourceOwnerName The user-recognizable name.
     * @param masterTimeout timeout to use with any request.
     * @param baseParameters The base parameters to specify for the request.
     * @param dirty Whether the resource is dirty or not
     */
    protected PublishableHttpResource(
        final String resourceOwnerName,
        @Nullable final TimeValue masterTimeout,
        final Map<String, String> baseParameters,
        final boolean dirty
    ) {
        super(resourceOwnerName, dirty);

        if (masterTimeout != null && TimeValue.MINUS_ONE.equals(masterTimeout) == false) {
            final Map<String, String> parameters = Maps.newMapWithExpectedSize(baseParameters.size() + 1);

            parameters.putAll(baseParameters);
            parameters.put("master_timeout", masterTimeout.toString());

            this.defaultParameters = Collections.unmodifiableMap(parameters);
        } else {
            this.defaultParameters = baseParameters;
        }
    }

    /**
     * Get the default parameters to use with every request.
     *
     * @return Never {@code null}.
     */
    public Map<String, String> getDefaultParameters() {
        return defaultParameters;
    }

    /**
     * Perform whatever is necessary to check and publish this {@link PublishableHttpResource}.
     *
     * @param client The REST client to make the request(s).
     * @param listener Returns {@code true} if the resource is available for use. {@code false} to stop.
     */
    @Override
    protected final void doCheckAndPublish(final RestClient client, final ActionListener<ResourcePublishResult> listener) {
        doCheck(client, ActionListener.wrap(exists -> {
            if (exists) {
                // it already exists, so we can skip publishing it
                listener.onResponse(ResourcePublishResult.ready());
            } else {
                doPublish(client, listener);
            }
        }, listener::onFailure));
    }

    /**
     * Determine if the current resource exists.
     *
     * @param client The REST client to make the request(s).
     * @param listener Returns {@code true} if the resource already available to use. {@code false} otherwise.
     */
    protected abstract void doCheck(RestClient client, ActionListener<Boolean> listener);

    /**
     * Determine if the current {@code resourceName} exists at the {@code resourceBasePath} endpoint with a version greater than or equal
     * to the expected version.
     * <p>
     * This provides the base-level check for any resource that does not need to care about its response beyond existence (and likely does
     * not need to inspect its contents).
     * <p>
     * This expects responses in the form of:
     * <pre><code>
     * {
     *   "resourceName": {
     *     "version": 6000002
     *   }
     * }
     * </code></pre>
     *
     * @param client The REST client to make the request(s).
     * @param listener Returns {@code true} if the resource was successfully published. {@code false} otherwise.
     * @param logger The logger to use for status messages.
     * @param resourceBasePath The base path/endpoint to check for the resource (e.g., "/_template").
     * @param resourceName The name of the resource (e.g., "template123").
     * @param resourceType The type of resource (e.g., "monitoring template").
     * @param resourceOwnerName The user-recognizeable resource owner.
     * @param resourceOwnerType The type of resource owner being dealt with (e.g., "monitoring cluster").
     * @param xContent The XContent used to parse the response.
     * @param minimumVersion The minimum version allowed without being replaced (expected to be the last updated version).
     */
    protected void versionCheckForResource(
        final RestClient client,
        final ActionListener<Boolean> listener,
        final Logger logger,
        final String resourceBasePath,
        final String resourceName,
        final String resourceType,
        final String resourceOwnerName,
        final String resourceOwnerType,
        final XContent xContent,
        final int minimumVersion
    ) {
        final CheckedFunction<Response, Boolean, IOException> responseChecker = (response) -> shouldReplaceResource(
            response,
            xContent,
            resourceName,
            minimumVersion
        );

        checkForResource(
            client,
            listener,
            logger,
            resourceBasePath,
            resourceName,
            resourceType,
            resourceOwnerName,
            resourceOwnerType,
            GET_EXISTS,
            GET_DOES_NOT_EXIST,
            responseChecker,
            this::alwaysReplaceResource
        );
    }

    /**
     * Determine if the current {@code resourceName} exists at the {@code resourceBasePath} endpoint.
     * <p>
     * This provides the base-level check for any resource that cares about existence and also its contents.
     *
     * @param client The REST client to make the request(s).
     * @param listener Returns {@code true} if the resource was successfully published. {@code false} otherwise.
     * @param logger The logger to use for status messages.
     * @param resourceBasePath The base path/endpoint to check for the resource (e.g., "/_template"), if any.
     * @param resourceName The name of the resource (e.g., "template123").
     * @param resourceType The type of resource (e.g., "monitoring template").
     * @param resourceOwnerName The user-recognizeable resource owner.
     * @param resourceOwnerType The type of resource owner being dealt with (e.g., "monitoring cluster").
     * @param exists Response codes that represent {@code EXISTS}.
     * @param doesNotExist Response codes that represent {@code DOES_NOT_EXIST}.
     * @param responseChecker Returns {@code true} if the resource should be replaced.
     * @param doesNotExistResponseChecker Returns {@code true} if the resource should be replaced.
     */
    protected void checkForResource(
        final RestClient client,
        final ActionListener<Boolean> listener,
        final Logger logger,
        final String resourceBasePath,
        final String resourceName,
        final String resourceType,
        final String resourceOwnerName,
        final String resourceOwnerType,
        final Set<Integer> exists,
        final Set<Integer> doesNotExist,
        final CheckedFunction<Response, Boolean, IOException> responseChecker,
        final CheckedFunction<Response, Boolean, IOException> doesNotExistResponseChecker
    ) {
        logger.trace("checking if {} [{}] exists on the [{}] {}", resourceType, resourceName, resourceOwnerName, resourceOwnerType);

        final Request request = new Request("GET", resourceBasePath + "/" + resourceName);
        addDefaultParameters(request);

        // avoid exists and DNE parameters from being an exception by default
        final Set<Integer> expectedResponseCodes = Sets.union(exists, doesNotExist);
        request.addParameter("ignore", expectedResponseCodes.stream().map(i -> i.toString()).collect(Collectors.joining(",")));

        client.performRequestAsync(request, new ResponseListener() {

            @Override
            public void onSuccess(final Response response) {
                try {
                    final int statusCode = response.getStatusLine().getStatusCode();

                    // checking the content is the job of whoever called this function by checking the tuple's response
                    if (exists.contains(statusCode)) {
                        logger.debug("{} [{}] found on the [{}] {}", resourceType, resourceName, resourceOwnerName, resourceOwnerType);

                        // if we should replace it -- true -- then the resource "does not exist" as far as the caller is concerned
                        listener.onResponse(false == responseChecker.apply(response));

                    } else if (doesNotExist.contains(statusCode)) {
                        logger.debug(
                            "{} [{}] does not exist on the [{}] {}",
                            resourceType,
                            resourceName,
                            resourceOwnerName,
                            resourceOwnerType
                        );

                        // if we should replace it -- true -- then the resource "does not exist" as far as the caller is concerned
                        listener.onResponse(false == doesNotExistResponseChecker.apply(response));
                    } else {
                        onFailure(new ResponseException(response));
                    }
                } catch (Exception e) {
                    logger.error(
                        (Supplier<?>) () -> new ParameterizedMessage(
                            "failed to parse [{}/{}] on the [{}]",
                            resourceBasePath,
                            resourceName,
                            resourceOwnerName
                        ),
                        e
                    );

                    onFailure(e);
                }
            }

            @Override
            public void onFailure(final Exception exception) {
                if (exception instanceof ResponseException) {
                    final Response response = ((ResponseException) exception).getResponse();
                    final int statusCode = response.getStatusLine().getStatusCode();

                    logger.error(
                        (Supplier<?>) () -> new ParameterizedMessage(
                            "failed to verify {} [{}] on the [{}] {} with status code [{}]",
                            resourceType,
                            resourceName,
                            resourceOwnerName,
                            resourceOwnerType,
                            statusCode
                        ),
                        exception
                    );
                } else {
                    logger.error(
                        (Supplier<?>) () -> new ParameterizedMessage(
                            "failed to verify {} [{}] on the [{}] {}",
                            resourceType,
                            resourceName,
                            resourceOwnerName,
                            resourceOwnerType
                        ),
                        exception
                    );
                }

                listener.onFailure(exception);
            }

        });
    }

    /**
     * Publish the current resource.
     * <p>
     * This is only invoked if {@linkplain #doCheck(RestClient, ActionListener) the check} fails.
     *
     * @param client The REST client to make the request(s).
     * @param listener Returns {@code true} if the resource is available to use. Otherwise {@code false}.
     */
    protected abstract void doPublish(RestClient client, ActionListener<ResourcePublishResult> listener);

    /**
     * Upload the {@code resourceName} to the {@code resourceBasePath} endpoint.
     *
     * @param client The REST client to make the request(s).
     * @param listener Returns {@code true} if the resource was successfully published. {@code false} otherwise.
     * @param logger The logger to use for status messages.
     * @param resourceBasePath The base path/endpoint to check for the resource (e.g., "/_template").
     * @param resourceName The name of the resource (e.g., "template123").
     * @param parameters Map of query string parameters, if any.
     * @param body The {@link HttpEntity} that makes up the body of the request.
     * @param resourceType The type of resource (e.g., "monitoring template").
     * @param resourceOwnerName The user-recognizeable resource owner.
     * @param resourceOwnerType The type of resource owner being dealt with (e.g., "monitoring cluster").
     */
    protected void putResource(
        final RestClient client,
        final ActionListener<ResourcePublishResult> listener,
        final Logger logger,
        final String resourceBasePath,
        final String resourceName,
        final Map<String, String> parameters,
        final java.util.function.Supplier<HttpEntity> body,
        final String resourceType,
        final String resourceOwnerName,
        final String resourceOwnerType
    ) {
        logger.trace("uploading {} [{}] to the [{}] {}", resourceType, resourceName, resourceOwnerName, resourceOwnerType);

        final Request request = new Request("PUT", resourceBasePath + "/" + resourceName);
        addDefaultParameters(request);
        addParameters(request, parameters);
        request.setEntity(body.get());

        client.performRequestAsync(request, new ResponseListener() {

            @Override
            public void onSuccess(final Response response) {
                final int statusCode = response.getStatusLine().getStatusCode();

                // 200 or 201
                if (statusCode == RestStatus.OK.getStatus() || statusCode == RestStatus.CREATED.getStatus()) {
                    logger.debug("{} [{}] uploaded to the [{}] {}", resourceType, resourceName, resourceOwnerName, resourceOwnerType);

                    listener.onResponse(ResourcePublishResult.ready());
                } else {
                    onFailure(new RuntimeException("[" + resourceBasePath + "/" + resourceName + "] responded with [" + statusCode + "]"));
                }
            }

            @Override
            public void onFailure(final Exception exception) {
                logger.error(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "failed to upload {} [{}] on the [{}] {}",
                        resourceType,
                        resourceName,
                        resourceOwnerName,
                        resourceOwnerType
                    ),
                    exception
                );

                listener.onFailure(exception);
            }

        });
    }

    /**
     * Delete the {@code resourceName} using the {@code resourceBasePath} endpoint.
     * <p>
     * Note to callers: this will add an "ignore" parameter to the request so that 404 is not an exception and therefore considered
     * successful if it's not found. You can override this behavior by specifying any valid value for "ignore", at which point 404
     * responses will result in {@code false} and logged failure.
     *
     * @param client The REST client to make the request(s).
     * @param listener Returns {@code true} if it successfully deleted the item; <em>never</em> {@code false}.
     * @param logger The logger to use for status messages.
     * @param resourceBasePath The base path/endpoint to check for the resource (e.g., "/_template").
     * @param resourceName The name of the resource (e.g., "template123").
     * @param resourceType The type of resource (e.g., "monitoring template").
     * @param resourceOwnerName The user-recognizeable resource owner.
     * @param resourceOwnerType The type of resource owner being dealt with (e.g., "monitoring cluster").
     */
    protected void deleteResource(
        final RestClient client,
        final ActionListener<Boolean> listener,
        final Logger logger,
        final String resourceBasePath,
        final String resourceName,
        final String resourceType,
        final String resourceOwnerName,
        final String resourceOwnerType
    ) {
        logger.trace("deleting {} [{}] from the [{}] {}", resourceType, resourceName, resourceOwnerName, resourceOwnerType);

        final Request request = new Request("DELETE", resourceBasePath + "/" + resourceName);
        addDefaultParameters(request);

        if (false == defaultParameters.containsKey("ignore")) {
            // avoid 404 being an exception by default
            request.addParameter("ignore", Integer.toString(RestStatus.NOT_FOUND.getStatus()));
        }

        client.performRequestAsync(request, new ResponseListener() {

            @Override
            public void onSuccess(Response response) {
                final int statusCode = response.getStatusLine().getStatusCode();

                // 200 or 404 (not found is just as good as deleting it!)
                if (statusCode == RestStatus.OK.getStatus() || statusCode == RestStatus.NOT_FOUND.getStatus()) {
                    logger.debug("{} [{}] deleted from the [{}] {}", resourceType, resourceName, resourceOwnerName, resourceOwnerType);

                    listener.onResponse(true);
                } else {
                    onFailure(new RuntimeException("[" + resourceBasePath + "/" + resourceName + "] responded with [" + statusCode + "]"));
                }
            }

            @Override
            public void onFailure(Exception exception) {
                logger.error(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "failed to delete {} [{}] on the [{}] {}",
                        resourceType,
                        resourceName,
                        resourceOwnerName,
                        resourceOwnerType
                    ),
                    exception
                );

                listener.onFailure(exception);
            }

        });
    }

    /**
     * Determine if the current resource should replaced the checked one based on its version (or lack thereof).
     * <p>
     * This expects a response like (where {@code resourceName} is replaced with its value):
     * <pre><code>
     * {
     *   "resourceName": {
     *     "version": 6000002
     *   }
     * }
     * </code></pre>
     *
     * @param response The filtered response from the _template/{name} or _ingest/pipeline/{name} resource APIs
     * @param xContent The XContent parser to use
     * @param resourceName The name of the looked up resource, which is expected to be the top-level key
     * @param minimumVersion The minimum version allowed without being replaced (expected to be the last updated version).
     * @return {@code true} represents that it should be replaced. {@code false} that it should be left alone.
     * @throws IOException if any issue occurs while parsing the {@code xContent} {@code response}.
     * @throws RuntimeException if the response format is changed.
     */
    protected boolean shouldReplaceResource(
        final Response response,
        final XContent xContent,
        final String resourceName,
        final int minimumVersion
    ) throws IOException {
        // no named content used; so EMPTY is fine
        final Map<String, Object> resources = XContentHelper.convertToMap(xContent, response.getEntity().getContent(), false);

        // if it's empty, then there's no version in the response thanks to filter_path
        if (resources.isEmpty() == false) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> resource = (Map<String, Object>) resources.get(resourceName);
            final Object version = resource != null ? resource.get("version") : null;

            // the version in the template is expected to include the alpha/beta/rc codes as well
            if (version instanceof Number) {
                return ((Number) version).intValue() < minimumVersion;
            }
        }

        return true;
    }

    /**
     * A useful placeholder for {@link CheckedFunction}s that want to always return {@code true}.
     *
     * @param response Unused.
     * @return Always {@code true}.
     */
    protected boolean alwaysReplaceResource(final Response response) {
        return true;
    }

    private void addDefaultParameters(final Request request) {
        this.addParameters(request, defaultParameters);
    }

    private void addParameters(final Request request, final Map<String, String> parameters) {
        for (final Map.Entry<String, String> param : parameters.entrySet()) {
            request.addParameter(param.getKey(), param.getValue());
        }
    }
}
