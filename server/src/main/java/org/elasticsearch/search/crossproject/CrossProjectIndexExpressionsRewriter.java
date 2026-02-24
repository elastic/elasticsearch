/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.crossproject;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.ClusterNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class for rewriting cross-project index expressions.
 * Provides methods that can rewrite qualified and unqualified index expressions to canonical CCS.
 */
public class CrossProjectIndexExpressionsRewriter {
    public static TransportVersion NO_MATCHING_PROJECT_EXCEPTION_VERSION = TransportVersion.fromName("no_matching_project_exception");
    public static final char EXCLUSION_PREFIX = '-';

    private static final Logger logger = LogManager.getLogger(CrossProjectIndexExpressionsRewriter.class);
    static final String[] MATCH_ALL = new String[] { Metadata.ALL };

    /**
     * Rewrites an index expression for cross-project search requests.
     * @param indexExpression the index expression to be rewritten to canonical CCS
     * @param originProjectAlias the alias of the origin project (can be null if it was excluded by project routing). It's passed
     *                           additionally to allProjectAliases because the origin project requires special handling:
     *                           it can match on its actual alias and on the special alias "_origin". Any expression matched by the origin
     *                           project also cannot be qualified with its actual alias in the final rewritten expression.
     * @param allProjectAliases the list of all project aliases (linked and origin) consider for a request
     * @param projectRouting the project routing that was applied to determine the origin and linked projects.
     *                       {@code null} if no project routing was applied.
     * @throws IllegalArgumentException if exclusions, date math or selectors are present in the index expressions
     * @throws NoMatchingProjectException if a qualified resource cannot be resolved because a project is missing
     */
    public static IndexRewriteResult rewriteIndexExpression(
        String indexExpression,
        @Nullable String originProjectAlias,
        Set<String> allProjectAliases,
        @Nullable String projectRouting
    ) {
        ensureProjectsAvailable(originProjectAlias, allProjectAliases, projectRouting);

        final boolean isQualified = RemoteClusterAware.isRemoteIndexName(indexExpression);
        final IndexRewriteResult rewrittenExpression;
        if (isQualified) {
            rewrittenExpression = rewriteQualifiedExpression(indexExpression, originProjectAlias, allProjectAliases, projectRouting);
            logger.debug("Rewrote qualified expression [{}] to [{}]", indexExpression, rewrittenExpression);
        } else {
            rewrittenExpression = rewriteUnqualifiedExpression(indexExpression, originProjectAlias, allProjectAliases);
            logger.debug("Rewrote unqualified expression [{}] to [{}]", indexExpression, rewrittenExpression);
        }
        // Empty rewritten expressions should have been thrown earlier
        assert false == rewrittenExpression.isEmpty() : "rewritten index expression must not be empty";
        return rewrittenExpression;
    }

    /**
     * Validates whether the index expression refers to an existing project without rewriting or expanding indices.
     * Throws {@link NoMatchingProjectException} if a qualified resource cannot be resolved because a project is missing.
     */
    public static void validateIndexExpressionWithoutRewrite(
        String indexExpression,
        @Nullable String originProjectAlias,
        Set<String> allProjectAliases,
        @Nullable String projectRouting
    ) {
        ensureProjectsAvailable(originProjectAlias, allProjectAliases, projectRouting);

        if (RemoteClusterAware.isRemoteIndexName(indexExpression) == false) {
            return;
        }

        String[] splitResource = RemoteClusterAware.splitIndexName(indexExpression);
        String requestedProjectAlias = splitResource[0];
        assert requestedProjectAlias != null : "Expected a project alias for a qualified resource but was null";
        if (isExclusionExpression(requestedProjectAlias)) {
            requestedProjectAlias = requestedProjectAlias.substring(1);
        }

        // triggers project alias resolving which will validate project aliases
        resolveProjectAliases(requestedProjectAlias, originProjectAlias, allProjectAliases, projectRouting);
    }

    static Set<String> getAllProjectAliases(@Nullable ProjectRoutingInfo originProject, List<ProjectRoutingInfo> linkedProjects) {
        assert originProject != null || linkedProjects.isEmpty() == false
            : "either origin project or linked projects must be in project target set";

        final Set<String> allProjectAliases = linkedProjects.stream().map(ProjectRoutingInfo::projectAlias).collect(Collectors.toSet());
        if (originProject != null) {
            allProjectAliases.add(originProject.projectAlias());
        }
        return Collections.unmodifiableSet(allProjectAliases);
    }

    private static IndexRewriteResult rewriteUnqualifiedExpression(
        String indexExpression,
        @Nullable String originAlias,
        Set<String> allProjectAliases
    ) {
        String localExpression = null;
        final Set<String> rewrittenExpressions = new LinkedHashSet<>();
        if (originAlias != null) {
            localExpression = indexExpression; // adding the original indexExpression for the _origin cluster.
        }
        for (String targetProjectAlias : allProjectAliases) {
            if (false == targetProjectAlias.equals(originAlias)) {
                rewrittenExpressions.add(RemoteClusterAware.buildRemoteIndexName(targetProjectAlias, indexExpression));
            }
        }
        return new IndexRewriteResult(localExpression, rewrittenExpressions);
    }

    private static IndexRewriteResult rewriteQualifiedExpression(
        String resource,
        @Nullable String originProjectAlias,
        Set<String> allProjectAliases,
        @Nullable String projectRouting
    ) {
        String[] splitResource = RemoteClusterAware.splitIndexName(resource);
        assert splitResource.length == 2
            : "Expected two strings (project and indexExpression) for a qualified resource ["
                + resource
                + "], but found ["
                + splitResource.length
                + "]";
        String requestedProjectAlias = splitResource[0];
        assert requestedProjectAlias != null : "Expected a project alias for a qualified resource but was null";
        boolean isExclusion = false;
        if (isExclusionExpression(requestedProjectAlias)) {
            // TODO: Throw exception on empty alias instead of relying on exception from resolveClusterNames?
            requestedProjectAlias = requestedProjectAlias.substring(1);
            isExclusion = true;
        }

        final String indexExpression = splitResource[1];
        if (isExclusion && isExclusionExpression(indexExpression)) {
            throw new IllegalArgumentException("Cannot apply exclusion for both the project and the index expression [" + resource + "]");
        }

        List<String> allProjectsMatchingAlias = resolveProjectAliases(
            requestedProjectAlias,
            originProjectAlias,
            allProjectAliases,
            projectRouting
        );

        String localExpression = null;
        final Set<String> resourcesMatchingLinkedProjectAliases = new LinkedHashSet<>();
        // TODO: Rewrite supports exclusion such as -project:index but it is still rejected by RemoteClusterAware#groupClusterIndices
        // We could consider supporting it all the way through, see also ES-13767
        for (String project : allProjectsMatchingAlias) {
            if (project.equals(originProjectAlias)) {
                localExpression = isExclusion ? EXCLUSION_PREFIX + indexExpression : indexExpression;
            } else {
                final String remoteIndexName = RemoteClusterAware.buildRemoteIndexName(project, indexExpression);
                resourcesMatchingLinkedProjectAliases.add(isExclusion ? EXCLUSION_PREFIX + remoteIndexName : remoteIndexName);
            }
        }

        return new IndexRewriteResult(localExpression, resourcesMatchingLinkedProjectAliases);
    }

    private static void ensureProjectsAvailable(
        @Nullable String originProjectAlias,
        Set<String> allProjectAliases,
        @Nullable String projectRouting
    ) {
        // Always 404 when no project is available for index resolution. This is matching error handling behaviour for resolving
        // projects with qualified index patterns such as "missing-*:index".
        if (originProjectAlias == null && allProjectAliases.isEmpty()) {
            assert projectRouting != null;
            throw new NoMatchingProjectException("no matching project after applying project routing [" + projectRouting + "]");
        }
    }

    private static List<String> resolveProjectAliases(
        String requestedProjectAlias,
        @Nullable String originProjectAlias,
        Set<String> allProjectAliases,
        @Nullable String projectRouting
    ) {
        if (ProjectRoutingResolver.ORIGIN.equals(requestedProjectAlias)) {
            // handling case where we have a qualified expression like: _origin:indexName
            if (originProjectAlias == null) {
                // handling case where we have a qualified expression like: _origin:indexName but no _origin project is set
                // This applies to exclusion as well, e.g. -_origin:indexName
                throw new NoMatchingProjectException(requestedProjectAlias, projectRouting);
            }
            return List.of(originProjectAlias);
        }

        try {
            // TODO: resolveClusterNames and the subsequent isEmpty check ensure 404 is thrown for any unmatched project.
            // This is different from index resolution where unmatched indices can be ignored. We may want to revisit
            // the different difference between the two in the future, see also ES-13766
            List<String> allProjectsMatchingAlias = ClusterNameExpressionResolver.resolveClusterNames(
                allProjectAliases,
                requestedProjectAlias
            );

            if (allProjectsMatchingAlias.isEmpty()) {
                throw new NoMatchingProjectException(requestedProjectAlias, projectRouting);
            }

            return allProjectsMatchingAlias;
        } catch (NoSuchRemoteClusterException ex) {
            logger.debug(ex.getMessage(), ex);
            throw new NoMatchingProjectException(requestedProjectAlias, projectRouting);
        }
    }

    static boolean isExclusionExpression(String expression) {
        assert expression != null && expression.isEmpty() == false : "expression must be a non-empty string";
        return expression.charAt(0) == EXCLUSION_PREFIX;
    }

    /**
     * A container for a local expression and a list of remote expressions.
     */
    public record IndexRewriteResult(@Nullable String localExpression, Set<String> remoteExpressions) {
        public IndexRewriteResult(String localExpression) {
            this(localExpression, Set.of());
        }

        public boolean isEmpty() {
            return localExpression == null && remoteExpressions.isEmpty();
        }
    }
}
