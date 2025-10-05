/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.crossproject;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.transport.TransportRequest;

/**
 * Utility class to determine whether Cross-Project Search (CPS) applies to an inbound request.
 * <p>
 * CPS applicability is controlled at three levels:
 * <ul>
 *   <li><b>Cluster level:</b> The {@code serverless.cross_project.enabled} setting determines
 *       whether CPS processing is available at all. In the future, all Serverless projects
 *       will support CPS, so this distinction will depend on whether the cluster is a
 *       Serverless cluster or not.</li>
 *   <li><b>API level:</b> The {@link org.elasticsearch.action.IndicesRequest.Replaceable#allowsCrossProject()}
 *       method determines whether a particular request type supports CPS processing.</li>
 *   <li><b>Request level:</b> An {@link org.elasticsearch.action.support.IndicesOptions} flag
 *       determines whether CPS should apply to the current
 *       request being processed. This fine-grained control is required because APIs that
 *       support CPS may also be used in contexts where CPS should not apply—for example,
 *       internal searches against the security system index to retrieve user roles, or CPS
 *       actions that execute in a flow where a parent action has already performed CPS
 *       processing.</li>
 * </ul>
 */
public final class CrossProjectModeDecider {
    private CrossProjectModeDecider() {}

    public static boolean isCrossProject(Settings settings) {
        return settings.getAsBoolean("serverless.cross_project.enabled", false);
    }

    public static boolean resolvesCrossProject(IndicesRequest.Replaceable request) {
        // TODO this needs to be based on the IndicesOptions flag instead, once available
        final boolean indicesOptionsResolveCrossProject = Booleans.parseBoolean(System.getProperty("cps.resolve_cross_project", "false"));
        return request.allowsCrossProject() && indicesOptionsResolveCrossProject;
    }

    public static boolean transportRequestResolvesCrossProject(TransportRequest request) {
        return request instanceof IndicesRequest.Replaceable replaceable && resolvesCrossProject(replaceable);
    }

    // TODO doesn't belong here
    public static IndicesOptions fanoutRequestIndicesOptions(IndicesOptions indicesOptions) {
        // TODO set resolveCrossProject=false here once we have an IndicesOptions flag for that
        return IndicesOptions.builder(indicesOptions)
            .concreteTargetOptions(new IndicesOptions.ConcreteTargetOptions(true))
            .wildcardOptions(IndicesOptions.WildcardOptions.builder(indicesOptions.wildcardOptions()).allowEmptyExpressions(true).build())
            .build();
    }
}
