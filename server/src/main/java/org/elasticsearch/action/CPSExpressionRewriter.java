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

public class CPSExpressionRewriter {
    private static final Logger logger = LogManager.getLogger(CPSExpressionRewriter.class);
    private static final String WILDCARD = "*";

    public static void maybeRewriteCrossProjectResolvableRequest(
        RemoteClusterAware remoteClusterAware,
        AuthorizedProjectsSupplier.AuthorizedProjects targetProjects,
        IndicesRequest.CrossProjectReplaceable request
    ) throws ResourceNotFoundException {
        if (targetProjects == AuthorizedProjectsSupplier.AuthorizedProjects.NOT_CROSS_PROJECT) {
            logger.info("Cross-project search is disabled or not applicable, skipping request [{}]...", request);
            return;
        }

        if (targetProjects.isOriginOnly()) {
            logger.info("Cross-project search is only for the origin project [{}], skipping rewrite...", targetProjects.origin());
            return;
        }

        if (targetProjects.projects().isEmpty()) {
            throw new ResourceNotFoundException("no target projects for cross-project search request");
        }

        String[] indices = request.indices();
        logger.info("Rewriting indices for CPS [{}]", Arrays.toString(indices));

        Map<String, List<String>> canonicalExpressionsMap = new LinkedHashMap<>(indices.length);
        for (String indexExpression : indices) {
            // TODO we need to handle exclusions here already
            boolean isQualified = isQualifiedIndexExpression(indexExpression);
            if (isQualified) {
                // TODO handle empty case here -- empty means "search all" in ES which is _not_ what we want
                List<String> canonicalExpressions = rewriteQualified(indexExpression, targetProjects, remoteClusterAware);
                // could fail early here in ignore_unavailable and allow_no_indices strict mode if things are empty
                canonicalExpressionsMap.put(indexExpression, canonicalExpressions);
                logger.info("Rewrote qualified expression [{}] to [{}]", indexExpression, canonicalExpressions);
            } else {
                // un-qualified expression, i.e. flat-world
                List<String> canonicalExpressions = rewriteUnqualified(indexExpression, targetProjects.projects());
                canonicalExpressionsMap.put(indexExpression, canonicalExpressions);
                logger.info("Rewrote unqualified expression [{}] to [{}]", indexExpression, canonicalExpressions);
            }
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
        String indicesExpressions,
        AuthorizedProjectsSupplier.AuthorizedProjects targetProjects,
        RemoteClusterAware remoteClusterAware
    ) {
        String[] splitExpression = RemoteClusterAware.splitIndexName(indicesExpressions);
        if (targetProjects.origin() != null && targetProjects.origin().equals(splitExpression[0])) {
            // handling special case where we have a qualified expression like: _origin:indexName
            return List.of(splitExpression[1]);
        }
        final Map<String, List<String>> map = remoteClusterAware.groupClusterIndices(
            Set.copyOf(targetProjects.projects()),
            new String[] { indicesExpressions }
        );
        final List<String> local = map.remove(LOCAL_CLUSTER_GROUP_KEY);
        final List<String> remote = map.entrySet()
            .stream()
            .flatMap(e -> e.getValue().stream().map(v -> e.getKey() + REMOTE_CLUSTER_INDEX_SEPARATOR + v))
            .toList();
        assert local == null || local.isEmpty() : "local indices should not be present in the map, but were: " + local;
        if (WILDCARD.equals(splitExpression[0])) {
            // handing of special case where the original expression was: *:indexName that is a
            // qualified expression that includes the origin cluster and all linked projects.
            List<String> remoteIncludingOrigin = new ArrayList<>(remote.size() + 1);
            remoteIncludingOrigin.addAll(remote);
            remoteIncludingOrigin.add(splitExpression[1]);
            return remoteIncludingOrigin;
        }
        return remote;
    }

    public static boolean isQualifiedIndexExpression(String indexExpression) {
        return RemoteClusterAware.isRemoteIndexName(indexExpression);
    }
}
