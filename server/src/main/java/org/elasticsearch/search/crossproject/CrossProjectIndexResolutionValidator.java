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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ResolvedIndexExpression;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.fieldcaps.RemoteDatasetNotSupportedException;
import org.elasticsearch.action.fieldcaps.RemoteResourceNotSupportedException;
import org.elasticsearch.action.fieldcaps.RemoteViewNotSupportedException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE;
import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED;
import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS;
import static org.elasticsearch.search.crossproject.CrossProjectIndexExpressionsRewriter.isExclusionExpression;

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
public class CrossProjectIndexResolutionValidator {
    private static final Logger logger = LogManager.getLogger(CrossProjectIndexResolutionValidator.class);

    private Map<String, ResolutionFailure.Unauthorized> remoteAuthorizationFailures = null;
    private Map<String, List<String>> remoteUnauthorizedIndices = null;
    private ResolutionFailure.Unauthorized localAuthorizationFailure = null;
    private List<String> localUnauthorizedIndices = null;

    private final IndicesOptions indicesOptions;
    private final ResolvedIndexExpressions localResolvedExpressions;
    private final Map<String, ResolvedIndexExpressions> remoteResolvedExpressions;
    private final Map<String, Exception> remoteExceptions;

    private ResolutionFailure.NotFound notFoundFailure = null;

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
     * @param projectRouting            The project routing string from the request, can be null if request does not specify it
     * @param localResolvedExpressions  Resolution results from the origin project
     * @param remoteResolvedExpressions Resolution results from linked projects
     * @param remoteExceptions          Connection exceptions, etc., from linked projects; should be empty when all remote requests succeed
     * @return a {@link ElasticsearchException} if validation fails, null if validation passes
     */
    public static ElasticsearchException validate(
        IndicesOptions indicesOptions,
        @Nullable String projectRouting,
        ResolvedIndexExpressions localResolvedExpressions,
        Map<String, ResolvedIndexExpressions> remoteResolvedExpressions,
        Map<String, Exception> remoteExceptions
    ) {
        // Check for remote view/dataset exceptions that may not have been caught by the per-expression checks above.
        // This can happen for flat expressions where the resolved expressions don't include remote expressions for them.
        // Both kinds are collected and reported together so a query matching a remote view on one project and a remote
        // dataset on another surfaces both at once rather than whichever project's exception is iterated first.
        List<String> remoteViews = new ArrayList<>();
        List<String> remoteDatasets = new ArrayList<>();
        for (Exception remoteEx : remoteExceptions.values()) {
            Throwable cause = ExceptionsHelper.unwrapCause(remoteEx);
            // A remote that hosts both kinds already combined them into RemoteResourceNotSupportedException; a remote with
            // a single kind reports the per-kind exception. Collect from whichever shape arrived.
            if (cause instanceof RemoteResourceNotSupportedException resourceException) {
                remoteViews.addAll(resourceException.views());
                remoteDatasets.addAll(resourceException.datasets());
            } else if (cause instanceof RemoteViewNotSupportedException viewException) {
                remoteViews.addAll(viewException.views());
            } else if (cause instanceof RemoteDatasetNotSupportedException datasetException) {
                remoteDatasets.addAll(datasetException.datasets());
            }
        }
        if (remoteViews.isEmpty() == false || remoteDatasets.isEmpty() == false) {
            return new RemoteResourceNotSupportedException(remoteViews, remoteDatasets);
        }

        if (indicesOptions.allowNoIndices() && indicesOptions.ignoreUnavailable()) {
            logger.debug("Skipping index existence check in lenient mode");
            return null;
        }

        final boolean hasProjectRouting = Strings.isEmpty(projectRouting) == false;
        logger.debug(
            "Checking index existence for [{}] and [{}] with indices options [{}]{}",
            localResolvedExpressions,
            remoteResolvedExpressions,
            indicesOptions,
            hasProjectRouting ? " and project routing [" + projectRouting + "]" : ""
        );

        return new CrossProjectIndexResolutionValidator(
            indicesOptions,
            localResolvedExpressions,
            remoteResolvedExpressions,
            remoteExceptions
        ).validate();
    }

    private CrossProjectIndexResolutionValidator(
        IndicesOptions indicesOptions,
        ResolvedIndexExpressions localResolvedExpressions,
        Map<String, ResolvedIndexExpressions> remoteResolvedExpressions,
        Map<String, Exception> remoteExceptions
    ) {
        this.indicesOptions = indicesOptions;
        this.localResolvedExpressions = localResolvedExpressions;
        this.remoteResolvedExpressions = remoteResolvedExpressions;
        this.remoteExceptions = remoteExceptions;
    }

    private ElasticsearchException validate() {
        for (ResolvedIndexExpression localResolvedIndices : localResolvedExpressions.expressions()) {
            String originalExpression = localResolvedIndices.original();
            logger.debug("Checking replaced expression for original expression [{}]", originalExpression);

            // Check if this is a qualified resource (project:index pattern)
            boolean isQualifiedExpression = RemoteClusterAware.isRemoteIndexName(originalExpression);

            // Sort expressions to ensure behaviour is deterministic
            // TODO consider sorting during index re-writing, to avoid sorting here
            var remoteExpressions = localResolvedIndices.remoteExpressions().stream().sorted().toList();
            ResolvedIndexExpression.LocalExpressions localExpressions = localResolvedIndices.localExpressions();
            var localFailure = checkResolutionFailure(
                localExpressions,
                asOriginExpression(originalExpression),
                localResolvedExpressions.authorizationFailureTemplate()
            );

            if (isQualifiedExpression) {
                validateQualifiedExpression(originalExpression, remoteExpressions, localFailure);
            } else {
                validateUnqualifiedExpression(originalExpression, localExpressions, remoteExpressions, localFailure);
            }
        }

        return buildValidationException();
    }

    private void validateQualifiedExpression(String originalExpression, List<String> remoteExpressions, ResolutionFailure localFailure) {
        switch (localFailure) {
            case ResolutionFailure.Unauthorized unauthorized -> recordLocalAuthorizationFailure(unauthorized, originalExpression);
            case ResolutionFailure.NotFound notFound -> recordNotFoundFailure(notFound);
            case null -> {
            }
        }

        // qualified linked project expression
        for (String remoteExpression : remoteExpressions) {
            var splitResource = RemoteClusterAware.splitIndexName(remoteExpression);
            var projectAlias = splitResource.clusterAlias();
            var resource = splitResource.indexExpression();

            var remoteFailure = checkSingleRemoteExpression(projectAlias, resource, remoteExpression);
            if (remoteFailure != null) {
                recordRemoteFailure(remoteFailure, remoteExpression, projectAlias);
            }
        }
    }

    private void validateUnqualifiedExpression(
        String originalExpression,
        ResolvedIndexExpression.LocalExpressions localExpressions,
        List<String> remoteExpressions,
        ResolutionFailure localFailure
    ) {
        if (localFailure == null && localExpressions != ResolvedIndexExpression.LocalExpressions.NONE) {
            // found locally, continue to next expression
            return;
        }
        assert localExpressions != ResolvedIndexExpression.LocalExpressions.NONE || false == remoteExpressions.isEmpty()
            : "both local expression and remote expressions are empty which should have errored earlier at index rewriting time";

        var remoteResult = checkUnqualifiedRemoteExpressions(remoteExpressions);
        // ignore all remote exceptions if we successfully resolved the expression on any remote
        if (remoteResult.foundFlat) {
            return;
        }

        // Record the first failure we find, according to the following precedence:
        // - Local 403
        // - Remote 403
        // - Local 404
        // - Remote 404 (we should only encounter this if the local project is excluded from index resolution)
        if (localFailure instanceof ResolutionFailure.Unauthorized authorizationFailure) {
            recordLocalAuthorizationFailure(authorizationFailure, originalExpression);
        } else if (remoteResult.authorizationFailure != null) {
            recordRemoteFailure(remoteResult.authorizationFailure, remoteResult.expression, remoteResult.projectAlias);
        } else if (localFailure != null) {
            assert localFailure instanceof ResolutionFailure.NotFound;
            recordNotFoundFailure((ResolutionFailure.NotFound) localFailure);
        } else {
            assert false == remoteExpressions.isEmpty() : "expected remote expressions to be non-empty";
            recordNotFoundFailure(new ResolutionFailure.NotFound(remoteExpressions.getFirst()));
        }
    }

    @Nullable
    private ElasticsearchException buildValidationException() {
        if (localAuthorizationFailure == null && remoteAuthorizationFailures == null) {
            // if we have an expression (simplest example: "*,-*") that resolves to no indices, and `allow_no_indices=false`, we need to
            // return a 404
            if (notFoundFailure == null && indicesOptions.allowNoIndices() == false) {
                if (localResolvedExpressions.localIndicesEmptyOrMissing()
                    && remoteResolvedExpressions.values().stream().allMatch(ResolvedIndexExpressions::localIndicesEmptyOrMissing)) {
                    return new IndexNotFoundException(
                        localResolvedExpressions.expressions()
                            .stream()
                            .map(ResolvedIndexExpression::original)
                            .collect(Collectors.joining(","))
                    );
                }
            }
            // null when all resolved expressions are valid
            return notFoundFailure != null ? new IndexNotFoundException(notFoundFailure.expression()) : null;
        } else {
            var firstException = localAuthorizationFailure != null
                ? formatAuthorizationException(localAuthorizationFailure.errorTemplate(), localUnauthorizedIndices)
                : null;

            if (remoteAuthorizationFailures != null) {
                for (var e : remoteAuthorizationFailures.entrySet()) {
                    final var unauthorizedIndices = remoteUnauthorizedIndices.get(e.getKey());
                    assert unauthorizedIndices.isEmpty() == false;

                    var exception = formatAuthorizationException(e.getValue().errorTemplate(), unauthorizedIndices);
                    if (firstException == null) {
                        firstException = exception;
                    } else {
                        // if we have multiple authorization errors (i.e. from remotes), attach these as suppressed to the first
                        // authorization error
                        firstException.addSuppressed(exception);
                    }
                }
            }

            return firstException;
        }
    }

    private void recordLocalAuthorizationFailure(ResolutionFailure.Unauthorized authorizationFailure, String originalExpression) {
        if (localAuthorizationFailure == null) {
            localAuthorizationFailure = authorizationFailure;
            localUnauthorizedIndices = new ArrayList<>();
        }
        localUnauthorizedIndices.add(originalExpression);
    }

    private void recordNotFoundFailure(ResolutionFailure.NotFound failure) {
        if (notFoundFailure == null) notFoundFailure = failure;
    }

    private void recordRemoteFailure(ResolutionFailure remoteFailure, String remoteExpression, String projectAlias) {
        switch (remoteFailure) {
            case ResolutionFailure.Unauthorized authorizationFailure -> {
                if (remoteAuthorizationFailures == null) {
                    remoteAuthorizationFailures = new LinkedHashMap<>();
                    remoteUnauthorizedIndices = new HashMap<>();
                }
                remoteAuthorizationFailures.putIfAbsent(projectAlias, authorizationFailure);
                remoteUnauthorizedIndices.computeIfAbsent(projectAlias, k -> new ArrayList<>()).add(remoteExpression);
            }
            case ResolutionFailure.NotFound notFound -> recordNotFoundFailure(notFound);
        }
    }

    private static ElasticsearchSecurityException formatAuthorizationException(String template, List<String> unauthorizedIndices) {
        return new ElasticsearchSecurityException(
            Strings.replace(template, "-*", Strings.collectionToCommaDelimitedString(unauthorizedIndices)),
            RestStatus.FORBIDDEN
        );
    }

    public static IndicesOptions indicesOptionsForCrossProjectFanout(IndicesOptions indicesOptions) {
        return IndicesOptions.builder(indicesOptions)
            .concreteTargetOptions(new IndicesOptions.ConcreteTargetOptions(true))
            .wildcardOptions(IndicesOptions.WildcardOptions.builder(indicesOptions.wildcardOptions()).allowEmptyExpressions(true).build())
            .crossProjectModeOptions(IndicesOptions.CrossProjectModeOptions.DEFAULT)
            .build();
    }

    private ResolutionFailure checkSingleRemoteExpression(String projectAlias, String resource, String remoteExpression) {
        if (isExclusionExpression(projectAlias) || isExclusionExpression(resource)) {
            logger.debug("Skipping check for excluded remote expression [{}:{}]", projectAlias, resource);
            return null;
        }

        ResolvedIndexExpressions resolvedExpressionsInProject = remoteResolvedExpressions.get(projectAlias);
        /*
         * We look for an index in the linked projects only after we've ascertained that it does not exist
         * on the origin. However, if we couldn't find a valid entry for the same index in the resolved
         * expressions `Map<K,V>` from the linked projects, it could mean that we did not hear back from
         * the linked project due to some error that occurred on it. In such case, the scenario effectively
         * is identical to the one where we could not find an index anywhere.
         */
        if (resolvedExpressionsInProject == null) {
            // if we're missing results from the remote because of a remote connection error, report it (otherwise we'll silently ignore it)
            if (remoteExceptions.containsKey(projectAlias)) {
                return new ResolutionFailure.NotFound(remoteExpression);
            } else {
                assert localResolvedExpressions.expressions()
                    .stream()
                    .anyMatch(e -> e.remoteExpressions().stream().anyMatch(Strings.format("-%s:*", projectAlias)::equals))
                    : Strings.format("Expected cluster exclusion for %s", projectAlias);

                if (indicesOptions.allowNoIndices() == false) {
                    return new ResolutionFailure.NotFound(remoteExpression);
                } else {
                    return null;
                }
            }
        }

        ResolvedIndexExpression.LocalExpressions matchingExpression = findMatchingExpression(resolvedExpressionsInProject, resource);
        if (matchingExpression == null) {
            // assume that this is the result of sending an exclusion to the remote
            assert hasProjectExclusionPrefix(localResolvedExpressions, projectAlias)
                : Strings.format("Could not find matching project exclusion for missing remote expression %s", remoteExpression);

            if (indicesOptions.allowNoIndices() == false) {
                return new ResolutionFailure.NotFound(remoteExpression);
            } else {
                return null;
            }
        }

        return checkResolutionFailure(matchingExpression, remoteExpression, resolvedExpressionsInProject.authorizationFailureTemplate());
    }

    private static boolean hasProjectExclusionPrefix(ResolvedIndexExpressions localExpressions, String projectAlias) {
        final String aliasPrefix = "-" + projectAlias + ":";
        final String indexPrefix = projectAlias + ":-";
        return localExpressions.expressions()
            .stream()
            .anyMatch(e -> e.remoteExpressions().stream().anyMatch(r -> r.startsWith(aliasPrefix) || r.startsWith(indexPrefix)));
    }

    // TODO optimize with a precomputed Map<String, ResolvedIndexExpression.LocalExpressions> instead
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

    private static String asOriginExpression(String originalExpression) {
        var split = RemoteClusterAware.splitIndexName(originalExpression);
        if (split.clusterAlias() == null || split.clusterAlias().indexOf('*') == -1) {
            return originalExpression;
        }
        return RemoteClusterAware.buildRemoteIndexName("_origin", split.indexExpression());
    }

    private ResolutionFailure checkResolutionFailure(
        ResolvedIndexExpression.LocalExpressions localExpressions,
        String expression,
        @Nullable String authorizationFailureTemplate
    ) {
        assert false == (indicesOptions.allowNoIndices() && indicesOptions.ignoreUnavailable())
            : "Should not be checking index existence in lenient mode";

        var result = localExpressions.localIndexResolutionResult();

        if (indicesOptions.ignoreUnavailable() == false) {
            if (result == CONCRETE_RESOURCE_NOT_VISIBLE) {
                return new ResolutionFailure.NotFound(expression);
            } else if (result == CONCRETE_RESOURCE_UNAUTHORIZED) {
                assert authorizationFailureTemplate != null
                    : "ResolvedIndexExpression should have authorization failure template set when concrete index is unauthorized";
                return new ResolutionFailure.Unauthorized(authorizationFailureTemplate);
            }
        }

        if (indicesOptions.allowNoIndices() == false && result == SUCCESS && localExpressions.indices().isEmpty()) {
            return new ResolutionFailure.NotFound(expression);
        }

        return null;
    }

    private record UnqualifiedRemoteExpressionResult(
        @Nullable ResolutionFailure.Unauthorized authorizationFailure,
        @Nullable String projectAlias,
        @Nullable String expression,
        boolean foundFlat
    ) {}

    private UnqualifiedRemoteExpressionResult checkUnqualifiedRemoteExpressions(List<String> remoteExpressions) {
        var result = new UnqualifiedRemoteExpressionResult(null, null, null, false);

        for (String remoteExpression : remoteExpressions) {
            var splitResource = RemoteClusterAware.splitIndexName(remoteExpression);
            var projectAlias = splitResource.clusterAlias();
            var resource = splitResource.indexExpression();

            var remoteFailure = checkSingleRemoteExpression(projectAlias, resource, remoteExpression);
            if (remoteFailure == null) {
                // found flat expression somewhere
                return new UnqualifiedRemoteExpressionResult(null, null, null, true);
            }
            if (result.authorizationFailure == null && remoteFailure instanceof ResolutionFailure.Unauthorized authorizationFailure) {
                result = new UnqualifiedRemoteExpressionResult(authorizationFailure, projectAlias, remoteExpression, false);
            }
        }

        return result;
    }

    private sealed interface ResolutionFailure {
        record Unauthorized(String errorTemplate) implements ResolutionFailure {}

        record NotFound(String expression) implements ResolutionFailure {}
    }
}
