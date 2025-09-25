/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.crossproject;

import org.elasticsearch.cluster.metadata.ClusterNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.crossproject.ProjectRoutingInfo;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class for rewriting cross-project index expressions.
 * Provides methods that can rewrite qualified and unqualified index expressions to canonical CCS.
 */
public class CrossProjectIndexExpressionsRewriter {
    private static final Logger logger = LogManager.getLogger(CrossProjectIndexExpressionsRewriter.class);
    private static final String ORIGIN_PROJECT_KEY = "_origin";
    private static final String WILDCARD = "*";
    private static final String[] MATCH_ALL = new String[] { WILDCARD };
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
    public static Map<String, List<String>> rewriteIndexExpressions(
        ProjectRoutingInfo originProject,
        List<ProjectRoutingInfo> linkedProjects,
        final String[] originalIndices
    ) {
        final String[] indices;
        if (originalIndices == null || originalIndices.length == 0) { // handling of match all cases besides _all and `*`
            // TODO this should be Metadata.ALL
            indices = MATCH_ALL;
        } else {
            indices = originalIndices;
        }
        assert false == IndexNameExpressionResolver.isNoneExpression(indices)
            : "expression list is *,-* which effectively means a request that requests no indices";
        assert originProject != null || linkedProjects.isEmpty() == false
            : "either origin project or linked projects must be in project target set";

        Set<String> linkedProjectNames = linkedProjects.stream().map(ProjectRoutingInfo::projectAlias).collect(Collectors.toSet());
        Map<String, List<String>> canonicalExpressionsMap = new LinkedHashMap<>(indices.length);
        for (String resource : indices) {
            if (canonicalExpressionsMap.containsKey(resource)) {
                continue;
            }
            maybeThrowOnUnsupportedResource(resource);

            boolean isQualified = RemoteClusterAware.isRemoteIndexName(resource);
            if (isQualified) {
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

                List<String> canonicalExpressions = rewriteQualified(projectAlias, indexExpression, originProject, linkedProjectNames);

                canonicalExpressionsMap.put(resource, canonicalExpressions);
                logger.debug("Rewrote qualified expression [{}] to [{}]", resource, canonicalExpressions);
            } else {
                List<String> canonicalExpressions = rewriteUnqualified(resource, originProject, linkedProjects);
                canonicalExpressionsMap.put(resource, canonicalExpressions);
                logger.debug("Rewrote unqualified expression [{}] to [{}]", resource, canonicalExpressions);
            }
        }
        return canonicalExpressionsMap;
    }

    private static List<String> rewriteUnqualified(
        String indexExpression,
        @Nullable ProjectRoutingInfo origin,
        List<ProjectRoutingInfo> projects
    ) {
        List<String> canonicalExpressions = new ArrayList<>();
        if (origin != null) {
            canonicalExpressions.add(indexExpression); // adding the original indexExpression for the _origin cluster.
        }
        for (ProjectRoutingInfo targetProject : projects) {
            canonicalExpressions.add(RemoteClusterAware.buildRemoteIndexName(targetProject.projectAlias(), indexExpression));
        }
        return canonicalExpressions;
    }

    private static List<String> rewriteQualified(
        String requestedProjectAlias,
        String indexExpression,
        @Nullable ProjectRoutingInfo originProject,
        Set<String> allProjectAliases
    ) {
        if (originProject != null && ORIGIN_PROJECT_KEY.equals(requestedProjectAlias)) {
            // handling case where we have a qualified expression like: _origin:indexName
            return List.of(indexExpression);
        }

        if (originProject == null && ORIGIN_PROJECT_KEY.equals(requestedProjectAlias)) {
            // handling case where we have a qualified expression like: _origin:indexName but no _origin project is set
            throw new NoMatchingProjectException(requestedProjectAlias);
        }

        try {
            if (originProject != null) {
                allProjectAliases.add(originProject.projectAlias());
            }
            List<String> resourcesMatchingAliases = new ArrayList<>();
            List<String> allProjectsMatchingAlias = ClusterNameExpressionResolver.resolveClusterNames(
                allProjectAliases,
                requestedProjectAlias
            );

            if (allProjectsMatchingAlias.isEmpty()) {
                throw new NoMatchingProjectException(requestedProjectAlias);
            }

            for (String project : allProjectsMatchingAlias) {
                if (originProject != null && project.equals(originProject.projectAlias())) {
                    resourcesMatchingAliases.add(indexExpression);
                } else {
                    resourcesMatchingAliases.add(RemoteClusterAware.buildRemoteIndexName(project, indexExpression));
                }
            }

            return resourcesMatchingAliases;
        } catch (NoSuchRemoteClusterException ex) {
            logger.debug(ex.getMessage(), ex);
            throw new NoMatchingProjectException(requestedProjectAlias);
        }
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
