/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.crossproject;

import org.elasticsearch.cluster.metadata.ClusterNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class for rewriting cross-project index expressions.
 * Provides methods that can rewrite qualified and unqualified index expressions to canonical CCS.
 */
public class CrossProjectIndexExpressionsRewriter {
    private static final Logger logger = LogManager.getLogger(CrossProjectIndexExpressionsRewriter.class);
    private static final String ORIGIN_PROJECT_KEY = "_origin";
    private static final String[] MATCH_ALL = new String[] { Metadata.ALL };
    private static final String EXCLUSION = "-";
    private static final String DATE_MATH = "<";

    public record Result(@Nullable String local, HashSet<String> remote) {}

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
    public static List<Result> rewriteIndexExpressions(
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
        assert originProject != null || linkedProjects.isEmpty() == false
            : "either origin project or linked projects must be in project target set";

        Set<String> linkedProjectNames = linkedProjects.stream().map(ProjectRoutingInfo::projectAlias).collect(Collectors.toSet());
        List<Result> results = new ArrayList<>(indices.length);
        for (String resource : indices) {
            results.add(rewriteIndexExpression(resource, originProject == null ? null : originProject.projectAlias(), linkedProjectNames));
        }
        return results;
    }

    /**
     * Rewrites a single index expression for cross-project search requests.
     * Handles qualified and unqualified expressions and match-all cases will also hand exclusions in the future.
     *
     * @param originProjectAlias the _origin project with its alias
     * @return a map from original index expressions to lists of canonical index expressions
     * @throws IllegalArgumentException if exclusions, date math or selectors are present in the index expressions
     * @throws NoMatchingProjectException if a qualified resource cannot be resolved because a project is missing
     */
    public static Result rewriteIndexExpression(String resource, @Nullable String originProjectAlias, Set<String> linkedProjectAliases) {
        maybeThrowOnUnsupportedResource(resource);

        boolean isQualified = RemoteClusterAware.isRemoteIndexName(resource);

        Result result;
        if (isQualified) {
            result = rewriteQualified(resource, originProjectAlias, linkedProjectAliases);
            logger.info("Rewrote qualified expression [{}] to [{}]", resource, result);
        } else {
            result = rewriteUnqualified(resource, originProjectAlias, linkedProjectAliases);
            logger.info("Rewrote unqualified expression [{}] to [{}]", resource, result);
        }
        return result;
    }

    public static Result rewriteQualified(String resource, @Nullable String originProjectAlias, Set<String> linkedProjectNames) {
        String[] splitResource = RemoteClusterAware.splitIndexName(resource);
        assert splitResource.length == 2
            : "Expected two strings (project and indexExpression) for a qualified resource ["
                + resource
                + "], but found ["
                + splitResource.length
                + "]";
        String projectAlias = splitResource[0];
        assert projectAlias != null : "Expected a project alias for a qualified resource but was null";
        String indexExpression = splitResource[1];
        maybeThrowOnUnsupportedResource(indexExpression);

        if (originProjectAlias != null && ORIGIN_PROJECT_KEY.equals(projectAlias)) {
            // handling case where we have a qualified expression like: _origin:indexName
            return new Result(indexExpression, new HashSet<>());
        }

        if (originProjectAlias == null && ORIGIN_PROJECT_KEY.equals(projectAlias)) {
            // handling case where we have a qualified expression like: _origin:indexName but no _origin project is set
            throw new NoMatchingProjectException(projectAlias);
        }

        try {
            if (originProjectAlias != null) {
                linkedProjectNames.add(originProjectAlias);
            }
            HashSet<String> resourcesMatchingAliases = new LinkedHashSet<>();
            List<String> allProjectsMatchingAlias = ClusterNameExpressionResolver.resolveClusterNames(linkedProjectNames, projectAlias);

            if (allProjectsMatchingAlias.isEmpty()) {
                throw new NoMatchingProjectException(projectAlias);
            }

            boolean includeOrigin = false;
            for (String project : allProjectsMatchingAlias) {
                if (project.equals(originProjectAlias)) {
                    includeOrigin = true;
                } else {
                    resourcesMatchingAliases.add(RemoteClusterAware.buildRemoteIndexName(project, indexExpression));
                }
            }

            return new Result(includeOrigin ? indexExpression : null, resourcesMatchingAliases);
        } catch (NoSuchRemoteClusterException ex) {
            logger.debug(ex.getMessage(), ex);
            throw new NoMatchingProjectException(projectAlias);
        }
    }

    public static Result rewriteUnqualified(String indexExpression, @Nullable String originProjectAlias, Set<String> linkedProjects) {
        HashSet<String> remoteExpressions = new LinkedHashSet<>();
        for (String targetProject : linkedProjects) {
            remoteExpressions.add(RemoteClusterAware.buildRemoteIndexName(targetProject, indexExpression));
        }
        boolean includeOrigin = originProjectAlias != null;
        return new Result(includeOrigin ? indexExpression : null, remoteExpressions);
    }

    private static void maybeThrowOnUnsupportedResource(String resource) {
        // TODO To be handled in future PR.
        if (resource.startsWith(EXCLUSION)) {
            throw new IllegalArgumentException("Exclusions are not currently supported but was found in the expression [" + resource + "]");
        }
        if (resource.startsWith(DATE_MATH)) {
            throw new IllegalArgumentException("Date math are not currently supported but was found in the expression [" + resource + "]");
        }
        if (IndexNameExpressionResolver.hasSelectorSuffix(resource)) {
            throw new IllegalArgumentException("Selectors are not currently supported but was found in the expression [" + resource + "]");
        }
    }
}
