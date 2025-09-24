/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.RemoteIndexExpressions;
import org.elasticsearch.action.ResolvedIndexExpression;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS;

public class CrossProjectErrorsUtil {
    private static final Logger logger = LogManager.getLogger(CrossProjectErrorsUtil.class);
    private static final String WILDCARD = "*";

    public void crossProjectFanoutErrorHandling(
        IndicesOptions indicesOptions,
        ResolvedIndexExpressions localResolvedExpressions,
        RemoteIndexExpressions remoteResolvedExpressions
    ) {
        if (indicesOptions.allowNoIndices() && indicesOptions.ignoreUnavailable()) {
            // nothing to do since we're in lenient mode
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

    private static void checkAllowNoIndices(
        String indexAlias,
        String originalExpression,
        ResolvedIndexExpression localResolvedIndices,
        RemoteIndexExpressions remoteResolvedExpressions,
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
        RemoteIndexExpressions remoteResolvedExpressions,
        boolean isFlatWorldResource
    ) {
        ResolvedIndexExpression.LocalExpressions localExpressions = localResolvedIndices.localExpressions();
        boolean existsLocally = false == localExpressions.expressions().isEmpty()
            && localExpressions.localIndexResolutionResult() == SUCCESS;

        if (existsLocally && (isFlatWorldResource || localExpressions.expressions().size() == 1)) {
            // a concrete index for the flat-world expression was found locally
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
        boolean foundFlat = false;
        int foundQualified = 0;
        for (var linkedProjectExpressions : remoteResolvedExpressions.expressions().values()) {
            // for each linked project we check if the resolved expressions contains the original expression and check for resolution status
            ResolvedIndexExpression.LocalExpressions resolvedRemoteExpression = linkedProjectExpressions.resolvedExpressions()
                .get(originalExpression);
            Set<String> remoteExpressions = resolvedRemoteExpression.expressions();
            assert remoteExpressions != null : "we should always have replaced expressions from remote";
            logger.debug("Replaced indices from remote response resolved: [{}]", remoteExpressions);
            boolean existsRemotely = false == remoteExpressions.isEmpty()
                && resolvedRemoteExpression.localIndexResolutionResult() == SUCCESS;
            if (existsRemotely) {
                if (isFlatWorldResource) {
                    logger.debug(
                        "Remote project has resolved entries for [{}], skipping further remote existence check",
                        originalExpression
                    );
                    foundFlat = true;
                    break;
                } else {
                    foundQualified++; // TODO MP check this. I am not sure.
                }
            } else if (resolvedRemoteExpression.exception() != null) {
                exceptions.add(resolvedRemoteExpression.exception());
            }
        }
        boolean missingFlatResource = isFlatWorldResource && false == foundFlat;
        boolean missingQualifiedResource = false == isFlatWorldResource && foundQualified < localResolvedIndices.remoteExpressions().size();

        if (missingFlatResource || missingQualifiedResource) {
            if (false == exceptions.isEmpty()) {
                // we only ever get exceptions if they are security related
                // back and forth on whether a mix or security and non-security (missing indices) exceptions should report
                // as 403 or 404
                ElasticsearchSecurityException e = new ElasticsearchSecurityException(
                    "authorization errors while resolving [" + originalExpression + "]",
                    RestStatus.FORBIDDEN
                );
                exceptions.forEach(e::addSuppressed);
                throw e;
            } else {
                // TODO composite exception based on missing resources
                throw new IndexNotFoundException(originalExpression);
            }
        }
    }
}
