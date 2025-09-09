/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.RemoteClusterService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CrossProjectSearchService {
    private static final Logger logger = LogManager.getLogger(CrossProjectSearchService.class);

    private final AuthorizedProjectsSupplier supplier;
    private final RemoteClusterService remoteClusterService;

    public CrossProjectSearchService(AuthorizedProjectsSupplier supplier, RemoteClusterService remoteClusterService) {
        this.supplier = supplier;
        this.remoteClusterService = remoteClusterService;
    }

    public boolean crossProjectMode() {
        return supplier.get() != AuthorizedProjectsSupplier.AuthorizedProjects.NOT_CROSS_PROJECT;
    }

    public Map<String, OriginalIndices> groupIndicesForFanoutAction(IndicesRequest.Replaceable replaceable) {
        return remoteClusterService.groupIndices(getIndicesOptions(replaceable.indicesOptions()), replaceable.indices());
    }

    private IndicesOptions getIndicesOptions(IndicesOptions indicesOptions) {
        return crossProjectMode() ? lenientIndicesOptions(indicesOptions) : indicesOptions;
    }

    private static IndicesOptions lenientIndicesOptions(IndicesOptions indicesOptions) {
        return IndicesOptions.builder(indicesOptions)
            .concreteTargetOptions(new IndicesOptions.ConcreteTargetOptions(true))
            .wildcardOptions(IndicesOptions.WildcardOptions.builder(indicesOptions.wildcardOptions()).allowEmptyExpressions(true).build())
            .build();
    }

    public <T extends ResponseWithReplacedIndexExpressions> void errorHandling(
        IndicesRequest.Replaceable request,
        Map<String, T> remoteResults
    ) {
        logger.info("Checking if we should throw for [{}] under CPS", request.getReplacedIndexExpressions());
        // No CPS nothing to do
        if (false == crossProjectMode()) {
            logger.info("Skipping because we are not in CPS mode...");
            return;
        }
        if (request.indicesOptions().allowNoIndices() && request.indicesOptions().ignoreUnavailable()) {
            // nothing to do since we're in lenient mode
            logger.info("Skipping index existence check in lenient mode");
            return;
        }

        Map<String, ReplacedIndexExpression> replacedExpressions = request.getReplacedIndexExpressions().replacedExpressionMap();
        assert replacedExpressions != null;
        logger.info("Replaced expressions to check: [{}]", replacedExpressions);
        for (ReplacedIndexExpression replacedIndexExpression : replacedExpressions.values()) {
            // TODO need to handle qualified expressions here, too
            String original = replacedIndexExpression.original();
            List<ElasticsearchException> exceptions = new ArrayList<>();
            boolean exists = replacedIndexExpression.hasLocalIndices()
                && replacedIndexExpression.resolutionResult() == ReplacedIndexExpression.ResolutionResult.SUCCESS;
            if (exists) {
                logger.info("Local cluster has canonical expression for [{}], skipping remote existence check", original);
                continue;
            }
            if (replacedIndexExpression.authorizationError() != null) {
                exceptions.add(replacedIndexExpression.authorizationError());
            }

            for (var remoteResponse : remoteResults.values()) {
                logger.info("Remote response resolved: [{}]", remoteResponse);
                Map<String, ReplacedIndexExpression> resolved = remoteResponse.getReplacedIndexExpressions().replacedExpressionMap();
                assert resolved != null;
                var r = resolved.get(original);
                if (r != null
                    && replacedIndexExpression.resolutionResult() == ReplacedIndexExpression.ResolutionResult.SUCCESS
                    && resolved.get(original).replacedBy().isEmpty() == false) {
                    logger.info("Remote cluster has resolved entries for [{}], skipping further remote existence check", original);
                    exists = true;
                    break;
                } else if (r != null && r.authorizationError() != null) {
                    assert r.resolutionResult() == ReplacedIndexExpression.ResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED
                        : "we should never get an error if we are authorized";
                    exceptions.add(resolved.get(original).authorizationError());
                }
            }

            if (false == exists && false == request.indicesOptions().ignoreUnavailable()) {
                if (false == exceptions.isEmpty()) {
                    // we only ever get exceptions if they are security related
                    // back and forth on whether a mix or security and non-security (missing indices) exceptions should report
                    // as 403 or 404
                    ElasticsearchSecurityException e = new ElasticsearchSecurityException(
                        "authorization errors while resolving [" + original + "]",
                        RestStatus.FORBIDDEN
                    );
                    exceptions.forEach(e::addSuppressed);
                    throw e;
                } else {
                    // TODO composite exception based on missing resources
                    throw new IndexNotFoundException(original);
                }
            }
        }
    }
}
