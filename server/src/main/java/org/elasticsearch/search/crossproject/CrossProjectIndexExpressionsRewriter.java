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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class for rewriting cross-project index expressions.
 * Provides methods that can rewrite qualified and unqualified index expressions to canonical CCS.
 */
public class CrossProjectIndexExpressionsRewriter {
    public static TransportVersion NO_MATCHING_PROJECT_EXCEPTION_VERSION = TransportVersion.fromName("no_matching_project_exception");

    private static final Logger logger = LogManager.getLogger(CrossProjectIndexExpressionsRewriter.class);
    private static final String[] MATCH_ALL = new String[] { Metadata.ALL };
    private static final String EXCLUSION = "-";
    private static final String DATE_MATH = "<";

    /**
     * Rewrites index expressions for cross-project search requests.
     * Handles qualified and unqualified expressions and match-all cases will also hand exclusions in the future.
     *
     * @param originProject the _origin project with its alias
     * @param linkedProjects the list of linked and available projects to consider for a request
     * @param originalIndices the array of index expressions to be rewritten to canonical CCS
     * @return a map from original index expressions to lists of canonical index expressions
     * @throws IllegalArgumentException if exclusions, date math or selectors are present in the index expressions
     * @throws NoMatchingProjectException if a qualified resource cannot be resolved because a project is missing
     */
    // TODO remove me: only used in tests
    public static Map<String, IndexRewriteResult> rewriteIndexExpressions(
        ProjectRoutingInfo originProject,
        List<ProjectRoutingInfo> linkedProjects,
        final String[] originalIndices
    ) {
        final String[] indices;
        if (originalIndices == null || originalIndices.length == 0) { // handling of match all cases besides _all and `*`
            indices = MATCH_ALL;
        } else {
            indices = originalIndices;
        }
        assert false == IndexNameExpressionResolver.isNoneExpression(indices)
            : "expression list is *,-* which effectively means a request that requests no indices";

        final Set<String> allProjectAliases = getAllProjectAliases(originProject, linkedProjects);
        final String originProjectAlias = originProject != null ? originProject.projectAlias() : null;
        final Map<String, IndexRewriteResult> canonicalExpressionsMap = new LinkedHashMap<>(indices.length);
        for (String indexExpression : indices) {
            if (canonicalExpressionsMap.containsKey(indexExpression)) {
                continue;
            }
            canonicalExpressionsMap.put(
                indexExpression,
                rewriteIndexExpression(indexExpression, originProjectAlias, allProjectAliases, null)
            );
        }
        return canonicalExpressionsMap;
    }

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
        maybeThrowOnUnsupportedResource(indexExpression);

        // Always 404 when no project is available for index resolution. This is matching error handling behaviour for resolving
        // projects with qualified index patterns such as "missing-*:index".
        if (originProjectAlias == null && allProjectAliases.isEmpty()) {
            assert projectRouting != null;
            throw new NoMatchingProjectException("no matching project after applying project routing [" + projectRouting + "]");
        }

        final boolean isQualified = RemoteClusterAware.isRemoteIndexName(indexExpression);
        final IndexRewriteResult rewrittenExpression;
        if (isQualified) {
            rewrittenExpression = rewriteQualifiedExpression(indexExpression, originProjectAlias, allProjectAliases, projectRouting);
            logger.debug("Rewrote qualified expression [{}] to [{}]", indexExpression, rewrittenExpression);
        } else {
            rewrittenExpression = rewriteUnqualifiedExpression(indexExpression, originProjectAlias, allProjectAliases);
            logger.debug("Rewrote unqualified expression [{}] to [{}]", indexExpression, rewrittenExpression);
        }
        return rewrittenExpression;
    }

    private static Set<String> getAllProjectAliases(@Nullable ProjectRoutingInfo originProject, List<ProjectRoutingInfo> linkedProjects) {
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
        String indexExpression = splitResource[1];
        maybeThrowOnUnsupportedResource(indexExpression);

        if (originProjectAlias != null && ProjectRoutingResolver.ORIGIN.equals(requestedProjectAlias)) {
            // handling case where we have a qualified expression like: _origin:indexName
            return new IndexRewriteResult(indexExpression);
        }

        if (originProjectAlias == null && ProjectRoutingResolver.ORIGIN.equals(requestedProjectAlias)) {
            // handling case where we have a qualified expression like: _origin:indexName but no _origin project is set
            throw new NoMatchingProjectException(requestedProjectAlias, projectRouting);
        }

        try {
            List<String> allProjectsMatchingAlias = ClusterNameExpressionResolver.resolveClusterNames(
                allProjectAliases,
                requestedProjectAlias
            );

            if (allProjectsMatchingAlias.isEmpty()) {
                throw new NoMatchingProjectException(requestedProjectAlias, projectRouting);
            }

            String localExpression = null;
            final Set<String> resourcesMatchingLinkedProjectAliases = new LinkedHashSet<>();
            for (String project : allProjectsMatchingAlias) {
                if (project.equals(originProjectAlias)) {
                    localExpression = indexExpression;
                } else {
                    resourcesMatchingLinkedProjectAliases.add(RemoteClusterAware.buildRemoteIndexName(project, indexExpression));
                }
            }

            return new IndexRewriteResult(localExpression, resourcesMatchingLinkedProjectAliases);
        } catch (NoSuchRemoteClusterException ex) {
            logger.debug(ex.getMessage(), ex);
            throw new NoMatchingProjectException(requestedProjectAlias, projectRouting);
        }
    }

    private static void maybeThrowOnUnsupportedResource(String resource) {
        // TODO To be handled in future PR.
        if (resource.startsWith(EXCLUSION)) {
            throw new IllegalArgumentException("Exclusions are not currently supported but was found in the expression [" + resource + "]");
        }
        if (IndexNameExpressionResolver.hasSelectorSuffix(resource)) {
            throw new IllegalArgumentException("Selectors are not currently supported but was found in the expression [" + resource + "]");
        }
    }

    /**
     * A container for a local expression and a list of remote expressions.
     */
    public record IndexRewriteResult(@Nullable String localExpression, Set<String> remoteExpressions) {
        public IndexRewriteResult(String localExpression) {
            this(localExpression, Set.of());
        }
    }
}
