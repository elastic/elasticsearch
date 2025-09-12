/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
import static org.elasticsearch.transport.RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR;

public class CrossProjectResolverUtils {
    private static final Logger logger = LogManager.getLogger(CrossProjectResolverUtils.class);
    private static final String WILDCARD = "*";
    private static final String MATCH_ALL = "_ALL";
    private static final String EXCLUSION = "-";

    public static void maybeRewriteCrossProjectResolvableRequest(
        RemoteClusterAware remoteClusterAware,
        AuthorizedProjectsSupplier.AuthorizedProjects targetProjects,
        IndicesRequest.CrossProjectReplaceable request
    ) throws ResourceNotFoundException {
        if (targetProjects == AuthorizedProjectsSupplier.AuthorizedProjects.NOT_CROSS_PROJECT) {
            logger.debug("Cross-project search is disabled or not applicable, skipping request [{}]...", request);
            return;
        }

        if (targetProjects.isOriginOnly()) {
            logger.debug("Cross-project search is only for the origin project [{}], skipping rewrite...", targetProjects.origin());
            return;
        }

        if (targetProjects.projects().isEmpty()) {
            throw new ResourceNotFoundException("no target projects for cross-project search request");
        }

        String[] indices = request.indices();
        logger.debug("Rewriting indices for CPS [{}]", Arrays.toString(indices));

        if (indices.length == 0 || WILDCARD.equals(indices[0]) || MATCH_ALL.equalsIgnoreCase(indices[0])) {
            // handling of match all cases
            indices = new String[] { WILDCARD };
        }
        boolean atLeastOneResourceWasFound = true;
        Map<String, List<String>> canonicalExpressionsMap = new LinkedHashMap<>(indices.length);
        for (String resource : indices) {
            // TODO We will need to handle exclusions here. For now we are throwing instead if we see an exclusion.
            if (EXCLUSION.equals(resource)) {
                throw new IllegalArgumentException(
                    "Exclusions are not currently supported but was found in the expression [" + resource + "]"
                );
            }
            boolean isQualified = isQualifiedResource(resource);
            if (isQualified) {
                List<String> canonicalExpressions = rewriteQualified(resource, targetProjects, remoteClusterAware);
                // could fail early here in ignore_unavailable and allow_no_indices strict mode if things are empty
                canonicalExpressionsMap.put(resource, canonicalExpressions);
                if (canonicalExpressions.isEmpty() == false) {
                    atLeastOneResourceWasFound = false;
                }
                logger.debug("Rewrote qualified expression [{}] to [{}]", resource, canonicalExpressions);
            } else {
                atLeastOneResourceWasFound = false;
                // un-qualified expression, i.e. flat-world
                List<String> canonicalExpressions = rewriteUnqualified(resource, targetProjects.projects());
                canonicalExpressionsMap.put(resource, canonicalExpressions);
                logger.debug("Rewrote unqualified expression [{}] to [{}]", resource, canonicalExpressions);
            }
        }
        if (atLeastOneResourceWasFound) {
            // Do we want to throw in this case?
            throw new ResourceNotFoundException("no target projects for cross-project search request");
        }
        request.setCanonicalExpressions(canonicalExpressionsMap);
    }

    private static List<String> rewriteUnqualified(String indexExpression, List<String> projects) {
        List<String> canonicalExpressions = new ArrayList<>();
        canonicalExpressions.add(indexExpression);
        for (String targetProject : projects) {
            canonicalExpressions.add(RemoteClusterAware.buildRemoteIndexName(targetProject, indexExpression));
        }
        return canonicalExpressions;
    }

    private static List<String> rewriteQualified(
        String resource,
        AuthorizedProjectsSupplier.AuthorizedProjects targetProjects,
        RemoteClusterAware remoteClusterAware
    ) {
        String[] splitExpression = RemoteClusterAware.splitIndexName(resource);
        if (targetProjects.origin() != null && targetProjects.origin().equals(splitExpression[0])) {
            // handling special case where we have a qualified expression like: _origin:indexName
            return List.of(splitExpression[1]);
        }
        final Map<String, List<String>> map = remoteClusterAware.groupClusterIndices(
            Set.copyOf(targetProjects.projects()),
            new String[] { resource }
        );
        final List<String> local = map.remove(LOCAL_CLUSTER_GROUP_KEY);
        final List<String> remote = map.entrySet()
            .stream()
            .flatMap(e -> e.getValue().stream().map(v -> e.getKey() + REMOTE_CLUSTER_INDEX_SEPARATOR + v))
            .toList();
        assert local == null || local.isEmpty() : "local indices should not be present in the map, but were: " + local;
        if (WILDCARD.equals(splitExpression[0])) {
            // handing of special case where the original expression was: *:indexName that is a
            // qualified expression that includes the origin project and all linked projects.
            List<String> remoteIncludingOrigin = new ArrayList<>(remote.size() + 1);
            remoteIncludingOrigin.addAll(remote);
            remoteIncludingOrigin.add(splitExpression[1]);
            return remoteIncludingOrigin;
        }
        return remote;
    }

    public static boolean isQualifiedResource(String resource) {
        return RemoteClusterAware.isRemoteIndexName(resource);
    }
}
