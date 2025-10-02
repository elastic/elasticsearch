/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.crossproject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ResolvedIndexExpression;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE;
import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED;
import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS;

/**
 * Utility class for validating index resolution results in cross-project operations.
 * <p>
 * This class provides consistent error handling for scenarios where index resolution
 * spans multiple projects, taking into account the provided {@link IndicesOptions}.
 * It handles:
 * <ul>
 *   <li>Validation of index existence in both origin and linked projects based on IndicesOptions
 *       (ignoreUnavailable, allowNoIndices)</li>
 *   <li>Authorization issues during cross-project index resolution, returning appropriate
 *       {@link ElasticsearchSecurityException} responses</li>
 *   <li>Both flat (unqualified) and qualified index expressions (including "_origin:" prefixed indices)</li>
 *   <li>Wildcard index patterns that may resolve differently across projects</li>
 * </ul>
 * <p>
 * The validator examines both local and remote resolution results to determine the appropriate
 * error response, returning {@link IndexNotFoundException} for missing indices or
 * {@link ElasticsearchSecurityException} for authorization failures.
 */
public class ResponseValidator {
    private static final Logger logger = LogManager.getLogger(ResponseValidator.class);
    private static final String WILDCARD = "*";
    private static final String ORIGIN = "_origin"; // TODO use available constants

    /**
     * Validates the results of cross-project index resolution and returns appropriate exceptions based on the provided
     * {@link IndicesOptions}.
     * <p>
     * This method handles error scenarios when resolving indices across multiple projects:
     * <ul>
     *   <li>If both {@code ignoreUnavailable} and {@code allowNoIndices} are true, the method returns null without validation
     *       (lenient mode)</li>
     *   <li>For wildcard patterns that resolve to no indices, validates against {@code allowNoIndices}</li>
     *   <li>For concrete indices that don't exist, validates against {@code ignoreUnavailable}</li>
     *   <li>For indices with authorization issues, returns security exceptions</li>
     * </ul>
     * <p>
     * The method considers both flat (unqualified) and qualified index expressions, as well as
     * local and linked project resolution results when determining the appropriate error response.
     *
     * @param indicesOptions            Controls error behavior for missing indices
     * @param localResolvedExpressions  Resolution results from the origin project
     * @param remoteResolvedExpressions Resolution results from linked projects
     * @return a {@link ElasticsearchException} if validation fails, null if validation passes
     */
    public static ElasticsearchException validate(
        IndicesOptions indicesOptions,
        ResolvedIndexExpressions localResolvedExpressions,
        Map<String, ResolvedIndexExpressions> remoteResolvedExpressions
    ) {
        if (indicesOptions.allowNoIndices() && indicesOptions.ignoreUnavailable()) {
            logger.debug("Skipping index existence check in lenient mode");
            return null;
        }

        logger.debug("Running index existence check for [{}] and {}", localResolvedExpressions, remoteResolvedExpressions);
        for (ResolvedIndexExpression localResolvedIndices : localResolvedExpressions.expressions()) {
            String originalExpression = localResolvedIndices.original();
            logger.debug("Checking replaced expression for original expression [{}]", originalExpression);

            // Check if this is a qualified resource (project:index pattern)
            boolean isQualifiedResource = RemoteClusterAware.isRemoteIndexName(originalExpression);

            // Handle qualified resources specially
            if (isQualifiedResource) {
                String[] splitResource = splitQualifiedResource(originalExpression);
                String projectAlias = splitResource[0];

                // Special handling for _origin prefixed resources
                if (projectAlias.equals(ORIGIN)) {
                    ElasticsearchException error = validateOriginResource(
                        localResolvedIndices,
                        localResolvedExpressions,
                        originalExpression
                    );
                    if (error != null) {
                        return error;
                    }
                    continue;
                }
            }

            // Perform validation based on indices options
            ElasticsearchException error = validateResourceWithOptions(
                originalExpression,
                localResolvedIndices,
                remoteResolvedExpressions,
                false == isQualifiedResource
            );

            if (error != null) {
                return error;
            }
        }

        return null;
    }

    // TODO probably does not belong here
    public static boolean shouldResolveCrossProject(IndicesRequest.Replaceable request) {
        // TODO this needs to be based on the IndicesOptions flag instead, once available
        final boolean indicesOptionsResolveCrossProject = Booleans.parseBoolean(System.getProperty("cps.resolve_cross_project", "false"));
        return request.allowsCrossProject() && indicesOptionsResolveCrossProject;
    }

    public static IndicesOptions lenientIndicesOptions(IndicesOptions indicesOptions) {
        return IndicesOptions.builder(indicesOptions)
            .concreteTargetOptions(new IndicesOptions.ConcreteTargetOptions(true))
            .wildcardOptions(IndicesOptions.WildcardOptions.builder(indicesOptions.wildcardOptions()).allowEmptyExpressions(true).build())
            .build();
    }

    private static ElasticsearchException validateOriginResource(
        ResolvedIndexExpression localResolvedIndices,
        ResolvedIndexExpressions localResolvedExpressions,
        String originalExpression
    ) {
        ResolvedIndexExpression.LocalExpressions localExpressions = localResolvedIndices.localExpressions();
        boolean resourceFoundLocally = isResourceFoundLocally(localExpressions);

        if (resourceFoundLocally) {
            logger.debug("Local cluster has canonical expression for original expression [{}], skipping extra checks", originalExpression);
            return null;
        }

        logger.debug("Handling origin qualified expression [{}] with [{}]", originalExpression, localResolvedIndices);
        // Treat local resolution as a remote with alias "_origin" to avoid duplicating code
        var localAsRemote = Map.of(ORIGIN, localResolvedExpressions);
        return checkSingleRemoteExpression(
            localAsRemote,
            false,
            ORIGIN,
            localResolvedIndices.original().substring(ORIGIN.length() + 1), // Remove "_origin:" prefix
            originalExpression,
            new ArrayList<>()
        );
    }

    private static ElasticsearchException validateResourceWithOptions(
        String originalExpression,
        ResolvedIndexExpression localResolvedIndices,
        Map<String, ResolvedIndexExpressions> remoteResolvedExpressions,
        boolean isFlatWorldResource
    ) {
        ResolvedIndexExpression.LocalExpressions localExpressions = localResolvedIndices.localExpressions();

        // Early return if we have local resource that satisfies conditions
        if (isResourceFoundLocally(localExpressions) && (isFlatWorldResource || localExpressions.expressions().size() == 1)) {
            logger.debug("Local cluster has canonical expression for [{}], skipping remote check", originalExpression);
            return null;
        }

        // Track exceptions
        List<ElasticsearchException> exceptions = new ArrayList<>();
        if (localExpressions.exception() != null) {
            exceptions.add(localExpressions.exception());
        }

        // Process remote expressions
        for (String remoteExpression : localResolvedIndices.remoteExpressions()) {
            // remoteExpressions are always qualified
            assert RemoteClusterAware.isRemoteIndexName(remoteExpression);

            String[] splitResource = splitQualifiedResource(remoteExpression);
            ElasticsearchException error = checkSingleRemoteExpression(
                remoteResolvedExpressions,
                isFlatWorldResource,
                splitResource[0], // projectAlias
                splitResource[1], // resource
                remoteExpression,
                exceptions
            );

            // Return immediately on error for qualified resources
            if (error != null) {
                return error;
            }
        }

        // Return first exception if any were collected (for flat world resources)
        return exceptions.isEmpty() ? null : exceptions.get(0);
    }

    private static ElasticsearchException checkSingleRemoteExpression(
        Map<String, ResolvedIndexExpressions> remoteResolvedExpressions,
        boolean isFlatWorldResource,
        String projectAlias,
        String resource,
        String remoteExpression,
        List<ElasticsearchException> exceptions
    ) {
        // Get resolution results from the linked project
        ResolvedIndexExpressions resolvedExpressionsInProject = remoteResolvedExpressions.get(projectAlias);
        assert resolvedExpressionsInProject != null : "We should always have resolved expressions from linked project";

        // Find the matching expression in the linked project
        ResolvedIndexExpression.LocalExpressions matchingExpression = findMatchingExpression(resolvedExpressionsInProject, resource);
        if (matchingExpression == null) {
            return createNotFoundException(remoteExpression, exceptions);
        }

        // Check resolution result
        if (matchingExpression.localIndexResolutionResult() == SUCCESS) {
            // Successfully found if there are concrete expressions
            return matchingExpression.expressions().isEmpty() ? createNotFoundException(remoteExpression, exceptions) : null;
        }

        // Handle authorization and visibility failures
        return handleResolutionFailure(matchingExpression.localIndexResolutionResult(), remoteExpression, isFlatWorldResource, exceptions);
    }

    private static String[] splitQualifiedResource(String resource) {
        String[] splitResource = RemoteClusterAware.splitIndexName(resource);
        assert splitResource.length == 2
            : "Expected two strings (project and indexExpression) for a qualified resource ["
                + resource
                + "], but found ["
                + splitResource.length
                + "]";
        return splitResource;
    }

    private static boolean isResourceFoundLocally(ResolvedIndexExpression.LocalExpressions localExpressions) {
        return false == localExpressions.expressions().isEmpty() && localExpressions.localIndexResolutionResult() == SUCCESS;
    }

    private static ResolvedIndexExpression.LocalExpressions findMatchingExpression(
        ResolvedIndexExpressions projectExpressions,
        String resource
    ) {
        return projectExpressions.expressions()
            .stream()
            .filter(expr -> expr.original().equals(resource))
            .map(ResolvedIndexExpression::localExpressions)
            .findFirst()
            .orElse(null);
    }

    private static ElasticsearchException createNotFoundException(String expression, List<ElasticsearchException> exceptions) {
        ElasticsearchException exception = new IndexNotFoundException(expression);
        exceptions.forEach(exception::addSuppressed);
        return exception;
    }

    private static ElasticsearchException handleResolutionFailure(
        ResolvedIndexExpression.LocalIndexResolutionResult result,
        String expression,
        boolean isFlatWorldResource,
        List<ElasticsearchException> exceptions
    ) {
        ElasticsearchException exception;

        if (result == CONCRETE_RESOURCE_NOT_VISIBLE) {
            exception = new IndexNotFoundException(expression);
        } else if (result == CONCRETE_RESOURCE_UNAUTHORIZED) {
            exception = new ElasticsearchSecurityException(
                "authorization errors while resolving [" + expression + "]",
                RestStatus.FORBIDDEN
            );
        } else {
            return null;
        }

        if (isFlatWorldResource) {
            exceptions.add(exception);
            return null;
        } else {
            exceptions.forEach(exception::addSuppressed);
            return exception;
        }
    }
}
