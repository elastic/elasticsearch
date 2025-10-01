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
import org.elasticsearch.action.ResolvedIndexExpression;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE;
import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED;
import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.NONE;
import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS;

/**
 * Utility class for handling errors in cross-project index operations.
 * <p>
 * This class provides consistent error handling for scenarios where index resolution
 * spans multiple projects, taking into account the provided {@link IndicesOptions}.
 * It handles:
 * <ul>
 *   <li>Validation of index existence in the merged project view based on IndicesOptions (ignoreUnavailable,
 *   allowNoIndices)</li>
 *   <li>Authorization issues during cross-project index resolution</li>
 *   <li>Both flat (unqualified) and qualified index expressions</li>
 *   <li>Wildcard index patterns that may resolve differently across projects</li>
 * </ul>
 * <p>
 * The utility examines both local and remote resolution results to determine the appropriate
 * error response, throwing {@link IndexNotFoundException} for missing indices or
 * {@link ElasticsearchSecurityException} for authorization failures.
 */
public class CrossProjectSearchErrorHandler {
    private static final Logger logger = LogManager.getLogger(CrossProjectSearchErrorHandler.class);
    private static final String WILDCARD = "*";

    public static void crossProjectFanoutErrorHandling(
        IndicesOptions indicesOptions,
        ResolvedIndexExpressions primaryResolvedIndexExpressions,
        Map<String, ResolvedIndexExpressions> resolvedIndexExpressionsByRemote
    ) {
        Map<String, LinkedProjectExpressions> remoteLinkedProjectExpressions = resolvedIndexExpressionsByRemote.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> LinkedProjectExpressions.fromResolvedExpressions(e.getValue())));
        tempCrossProjectFanoutErrorHandling(indicesOptions, primaryResolvedIndexExpressions, remoteLinkedProjectExpressions);
    }

    /**
     * Validates the results of cross-project index resolution and throws appropriate exceptions based on the provided
     * {@link IndicesOptions}.
     * <p>
     * This method handles error scenarios when resolving indices across multiple projects:
     * <ul>
     *   <li>If both {@code ignoreUnavailable} and {@code allowNoIndices} are true, the method returns without validation
     *       (lenient mode)</li>
     *   <li>For wildcard patterns that resolve to no indices, validates against {@code allowNoIndices}</li>
     *   <li>For concrete indices that don't exist, validates against {@code ignoreUnavailable}</li>
     *   <li>For indices with authorization issues, throws security exceptions</li>
     * </ul>
     * <p>
     * The method considers both flat (unqualified) and qualified index expressions, as well as
     * local and linked project resolution results when determining whether to throw exceptions.
     *
     * @param indicesOptions            Controls error behavior for missing indices
     * @param localResolvedExpressions  Resolution results from the origin project
     * @param remoteResolvedExpressions Resolution results from linked projects
     * @throws IndexNotFoundException         If indices are missing and the {@code IndicesOptions} do not allow it
     * @throws ElasticsearchSecurityException If authorization errors occurred during index resolution
     */
    private static void tempCrossProjectFanoutErrorHandling(
        IndicesOptions indicesOptions,
        ResolvedIndexExpressions localResolvedExpressions,
        Map<String, LinkedProjectExpressions> remoteResolvedExpressions
    ) {
        logger.info(
            "Checking cross-project index resolution results for [{}] and [{}]",
            localResolvedExpressions,
            remoteResolvedExpressions
        );

        if (indicesOptions.allowNoIndices() && indicesOptions.ignoreUnavailable()) {
            logger.debug("Skipping index existence check in lenient mode");
            return;
        }

        for (ResolvedIndexExpression localResolvedIndices : localResolvedExpressions.expressions()) {
            String originalExpression = localResolvedIndices.original();

            logger.debug("Checking replaced expression for original expression [{}]", originalExpression);

            String resource = originalExpression;
            boolean isQualifiedResource = RemoteClusterAware.isRemoteIndexName(resource);
            if (isQualifiedResource) {
                // handle qualified resource eg. P1:logs*
                String[] splitResource = RemoteClusterAware.splitIndexName(resource);
                assert splitResource.length == 2
                    : "Expected two strings (project and indexExpression) for a qualified resource ["
                        + resource
                        + "], but found ["
                        + splitResource.length
                        + "]";
                resource = splitResource[1];
            }
            if (false == indicesOptions.allowNoIndices()) {
                checkAllowNoIndices(
                    resource,
                    originalExpression,
                    localResolvedIndices,
                    remoteResolvedExpressions,
                    isQualifiedResource == false
                );
            } else if (false == indicesOptions.ignoreUnavailable()) {
                checkIndicesOptions(originalExpression, localResolvedIndices, remoteResolvedExpressions, isQualifiedResource == false);
            }
        }
    }

    public static boolean resolveCrossProject(IndicesOptions indicesOptions) {
        // TODO this needs to be based on the IndicesOptions flag instead, once available
        return Boolean.parseBoolean(System.getProperty("cps.resolve_cross_project", "false"));
    }

    public static IndicesOptions lenientIndicesOptionsForCrossProject(IndicesOptions indicesOptions) {
        return IndicesOptions.builder(indicesOptions)
            .concreteTargetOptions(new IndicesOptions.ConcreteTargetOptions(true))
            .wildcardOptions(IndicesOptions.WildcardOptions.builder(indicesOptions.wildcardOptions()).allowEmptyExpressions(true).build())
            .build();
    }

    private static void checkAllowNoIndices(
        String indexAlias,
        String originalExpression,
        ResolvedIndexExpression localResolvedIndices,
        Map<String, LinkedProjectExpressions> remoteResolvedExpressions,
        boolean isFlatWorldResource
    ) {
        // strict behaviour of allowNoIndices checks if a wildcard expression resolves to no concrete indices.
        if (false == indexAlias.contains(WILDCARD)) {
            return;
        }
        checkIndicesOptions(originalExpression, localResolvedIndices, remoteResolvedExpressions, isFlatWorldResource);
    }

    private static void checkIndicesOptions(
        String originalExpression,
        ResolvedIndexExpression localResolvedIndices,
        Map<String, LinkedProjectExpressions> remoteResolvedExpressions,
        boolean isFlatWorldResource
    ) {
        ResolvedIndexExpression.LocalExpressions localExpressions = localResolvedIndices.localExpressions();
        boolean resourceFound = false == localExpressions.expressions().isEmpty()
            && (localExpressions.localIndexResolutionResult() == SUCCESS || localExpressions.localIndexResolutionResult() == NONE);

        if (resourceFound && (isFlatWorldResource || localExpressions.expressions().size() == 1)) {
            logger.info(
                "Local cluster has canonical expression for original expression [{}], skipping remote existence check",
                originalExpression
            );
            return;
        }
        List<ElasticsearchException> exceptions = new ArrayList<>();
        ElasticsearchException localException = localExpressions.exception();
        if (localException != null) {
            exceptions.add(localException);
        }

        if (localResolvedIndices.remoteExpressions().isEmpty()) {
            if (localExpressions.localIndexResolutionResult() == CONCRETE_RESOURCE_NOT_VISIBLE) {
                throw new IndexNotFoundException(originalExpression);
            }
            if (localExpressions.localIndexResolutionResult() == CONCRETE_RESOURCE_UNAUTHORIZED) {
                // we only ever get exceptions if they are security related
                // back and forth on whether a mix or security and non-security (missing indices) exceptions should report
                // as 403 or 404
                ElasticsearchSecurityException e = new ElasticsearchSecurityException(
                    "authorization errors while resolving [" + originalExpression + "]",
                    RestStatus.FORBIDDEN
                );
                exceptions.forEach(e::addSuppressed);
                throw e;
            }
        }

        for (String remoteExpression : localResolvedIndices.remoteExpressions()) {
            boolean isQualifiedResource = RemoteClusterAware.isRemoteIndexName(remoteExpression);
            if (isQualifiedResource) {
                // handle qualified resource eg. P1:logs*
                String[] splitResource = RemoteClusterAware.splitIndexName(remoteExpression);
                assert splitResource.length == 2
                    : "Expected two strings (project and indexExpression) for a qualified resource ["
                        + remoteExpression
                        + "], but found ["
                        + splitResource.length
                        + "]";
                String projectAlias = splitResource[0];
                String resource = splitResource[1];
                LinkedProjectExpressions linkedProjectExpressions = remoteResolvedExpressions.get(projectAlias);
                assert linkedProjectExpressions != null : "we should always have linked expressions from remote";

                ResolvedIndexExpression.LocalExpressions resolvedRemoteExpression = linkedProjectExpressions.resolvedExpressions()
                    .get(resource);
                assert resolvedRemoteExpression != null : "we should always have resolved expressions from remote";

                // TODO in wildcard case?
                if (resolvedRemoteExpression.expressions().isEmpty()) {
                    throw new IndexNotFoundException(originalExpression);
                }
                if (resolvedRemoteExpression.localIndexResolutionResult() == CONCRETE_RESOURCE_NOT_VISIBLE) {
                    throw new IndexNotFoundException(originalExpression);
                }
                if (resolvedRemoteExpression.localIndexResolutionResult() == CONCRETE_RESOURCE_UNAUTHORIZED) {
                    // we only ever get exceptions if they are security related
                    // back and forth on whether a mix or security and non-security (missing indices) exceptions should report
                    // as 403 or 404
                    ElasticsearchSecurityException e = new ElasticsearchSecurityException(
                        "authorization errors while resolving [" + remoteExpression + "]",
                        RestStatus.FORBIDDEN
                    );
                    exceptions.forEach(e::addSuppressed);
                    throw e;
                }
            } else {
                boolean foundFlat = false;
                for (var linkedProjectExpressions : remoteResolvedExpressions.values()) {
                    ResolvedIndexExpression.LocalExpressions resolvedRemoteExpression = linkedProjectExpressions.resolvedExpressions()
                        .get(remoteExpression);
                    if (resolvedRemoteExpression != null
                        && resolvedRemoteExpression.expressions().isEmpty() == false
                        && resolvedRemoteExpression.localIndexResolutionResult() == SUCCESS) {
                        foundFlat = true;
                        break;
                    }
                }
                if (false == foundFlat) {
                    throw new IndexNotFoundException(originalExpression);
                }
            }
        }
    }
}
