/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.AuthorizedProjectsSupplier;
import org.elasticsearch.action.CrossProjectReplacedIndexExpressions;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ReplacedIndexExpression;
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

    public static CrossProjectReplacedIndexExpressions maybeRewriteCrossProjectResolvableRequest(
        RemoteClusterAware remoteClusterAware,
        AuthorizedProjectsSupplier.AuthorizedProjects targetProjects,
        IndicesRequest.CrossProjectSearchCapable request
    ) {
        if (targetProjects == AuthorizedProjectsSupplier.AuthorizedProjects.NOT_CROSS_PROJECT) {
            logger.info("Cross-project search is disabled or not applicable, skipping request [{}]...", request);
            return null;
        }

        if (targetProjects.isOriginOnly()) {
            logger.info("Cross-project search is only for the origin project [{}], skipping rewrite...", targetProjects.originProject());
            return null;
        }

        if (targetProjects.linkedProjects().isEmpty()) {
            throw new ResourceNotFoundException("no target projects for cross-project search request");
        }

        List<String> projects = targetProjects.linkedProjects();
        String[] indices = request.indices();
        logger.info("Rewriting indices for CPS [{}]", Arrays.toString(indices));

        Map<String, ReplacedIndexExpression> replacedExpressions = new LinkedHashMap<>(indices.length);
        for (String indexExpression : indices) {
            // TODO we need to handle exclusions here already
            boolean isQualified = RemoteClusterAware.isRemoteIndexName(indexExpression);
            if (isQualified) {
                // TODO handle empty case here -- empty means "search all" in ES which is _not_ what we want
                List<String> canonicalExpressions = rewriteQualified(indexExpression, projects, remoteClusterAware);
                // could fail early here in ignore_unavailable and allow_no_indices strict mode if things are empty
                replacedExpressions.put(indexExpression, new ReplacedIndexExpression(indexExpression, canonicalExpressions));
                logger.info("Rewrote qualified expression [{}] to [{}]", indexExpression, canonicalExpressions);
            } else {
                // un-qualified expression, i.e. flat-world
                List<String> canonicalExpressions = rewriteUnqualified(indexExpression, targetProjects.linkedProjects());
                replacedExpressions.put(indexExpression, new ReplacedIndexExpression(indexExpression, canonicalExpressions));
                logger.info("Rewrote unqualified expression [{}] to [{}]", indexExpression, canonicalExpressions);
            }
        }

        return new CrossProjectReplacedIndexExpressions(replacedExpressions);
    }

    private static List<String> rewriteUnqualified(String indexExpression, List<String> projects) {
        List<String> canonicalExpressions = new ArrayList<>();
        canonicalExpressions.add(indexExpression);
        for (String targetProject : projects) {
            canonicalExpressions.add(RemoteClusterAware.buildRemoteIndexName(targetProject, indexExpression));
        }
        return canonicalExpressions;
    }

    private static List<String> rewriteQualified(String indicesExpressions, List<String> projects, RemoteClusterAware remoteClusterAware) {
        final Map<String, List<String>> map = remoteClusterAware.groupClusterIndices(
            Set.copyOf(projects),
            new String[] { indicesExpressions }
        );
        final List<String> local = map.remove(LOCAL_CLUSTER_GROUP_KEY);
        final List<String> remote = map.entrySet()
            .stream()
            .flatMap(e -> e.getValue().stream().map(v -> e.getKey() + REMOTE_CLUSTER_INDEX_SEPARATOR + v))
            .toList();
        assert local == null || local.isEmpty() : "local indices should not be present in the map, but were: " + local;
        return remote;
    }
}
