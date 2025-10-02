/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ResolvedIndexExpression;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;

import java.util.Map;
import java.util.Set;

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

        for (ResolvedIndexExpression localResolvedIndices : localResolvedExpressions.expressions()) {
            String originalExpression = localResolvedIndices.original();
            logger.debug("Checking replaced expression for original expression [{}]", originalExpression);

            // Check if this is a qualified resource (project:index pattern)
            boolean isQualifiedExpression = RemoteClusterAware.isRemoteIndexName(originalExpression);

            Set<String> remoteExpressions = localResolvedIndices.remoteExpressions();
            ResolvedIndexExpression.LocalExpressions localExpressions = localResolvedIndices.localExpressions();
            ResolvedIndexExpression.LocalIndexResolutionResult result = localExpressions.localIndexResolutionResult();
            if (isQualifiedExpression) {
                if (remoteExpressions.isEmpty()) {
                    // qualified local expression
                    ElasticsearchException e = handleResolutionFailure(result, originalExpression);
                    if (e == null) {
                        if (localExpressions.expressions().isEmpty()) {
                            return new IndexNotFoundException(originalExpression);
                        }
                    } else {
                        return e;
                    }
                } else {
                    // qualified linked project expression
                    for (String remoteExpression : remoteExpressions) {
                        String[] splitResource = splitQualifiedResource(remoteExpression);
                        ElasticsearchException exception = checkSingleRemoteExpression(
                            remoteResolvedExpressions,
                            splitResource[0], // projectAlias
                            splitResource[1], // resource
                            remoteExpression
                        );
                        if (exception != null) {
                            return exception;
                        }
                    }
                }
            } else {

                // flat local-only expression
                if (result == SUCCESS && false == localExpressions.expressions().isEmpty()) {
                    continue;
                }
                boolean isUnAuthorized = result == CONCRETE_RESOURCE_UNAUTHORIZED;
                // flat linked project expression
                boolean foundFlat = false;
                for (String remoteExpression : remoteExpressions) {
                    String[] splitResource = splitQualifiedResource(remoteExpression);
                    ElasticsearchException exception = checkSingleRemoteExpression(
                        remoteResolvedExpressions,
                        splitResource[0], // projectAlias
                        splitResource[1], // resource
                        remoteExpression
                    );
                    if (exception == null) {
                        // found flat expression somewhere
                        foundFlat = true;
                        break;
                    }
                    if (false == isUnAuthorized && exception instanceof ElasticsearchSecurityException) {
                        isUnAuthorized = true;
                    }
                }
                if (foundFlat) {
                    continue;
                }
                if (isUnAuthorized) {
                    return new ElasticsearchSecurityException("authorization errors while resolving [" + originalExpression + "]");
                }
                return new IndexNotFoundException(originalExpression);
            }
        }
        // if we didn't throw before it means that we can proceed with the request
        return null;
    }

    private static ElasticsearchException checkSingleRemoteExpression(
        Map<String, ResolvedIndexExpressions> remoteResolvedExpressions,
        String projectAlias,
        String resource,
        String remoteExpression
    ) {
        // Get resolution results from the linked project
        ResolvedIndexExpressions resolvedExpressionsInProject = remoteResolvedExpressions.get(projectAlias);
        assert resolvedExpressionsInProject != null : "We should always have resolved expressions from linked project";

        // Find the matching expression in the linked project
        ResolvedIndexExpression.LocalExpressions matchingExpression = findMatchingExpression(resolvedExpressionsInProject, resource);
        if (matchingExpression == null) {
            return new IndexNotFoundException(remoteExpression);
        }

        // Check resolution result
        if (matchingExpression.localIndexResolutionResult() == SUCCESS) {
            // Successfully found if there are concrete expressions
            return matchingExpression.expressions().isEmpty() ? new IndexNotFoundException(remoteExpression) : null;
        }

        // Handle authorization and visibility failures
        return handleResolutionFailure(matchingExpression.localIndexResolutionResult(), remoteExpression);
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

    private static ElasticsearchException handleResolutionFailure(
        ResolvedIndexExpression.LocalIndexResolutionResult result,
        String expression
    ) {
        if (result == CONCRETE_RESOURCE_NOT_VISIBLE) {
            return new IndexNotFoundException(expression);
        } else if (result == CONCRETE_RESOURCE_UNAUTHORIZED) {
            return new ElasticsearchSecurityException("authorization errors while resolving [" + expression + "]", RestStatus.FORBIDDEN);
        } else {
            return null;
        }
    }
}
