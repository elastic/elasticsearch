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
import org.elasticsearch.common.settings.Settings;

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
 *   <li><b>Request level:</b> The {@link org.elasticsearch.action.IndicesRequest.CrossProjectCandidate#resolvesCrossProject()}
 *       method determines whether CPS should apply to the current request being processed.
 *       For {@link org.elasticsearch.action.IndicesRequest} types this delegates to the
 *       {@link org.elasticsearch.action.support.IndicesOptions} flag. For other
 *       {@link org.elasticsearch.action.IndicesRequest.CrossProjectCandidate} types this is
 *       based on an explicit field set by the REST handler. This fine-grained control is
 *       required because APIs that support CPS may also be used in contexts where CPS should
 *       not apply—for example, internal searches against the security system index to retrieve
 *       user roles, ML datafeed scroll operations, or CPS actions that execute in a flow where
 *       a parent action has already performed CPS processing.</li>
 * </ul>
 */
public class CrossProjectModeDecider {
    private static final String CROSS_PROJECT_ENABLED_SETTING_KEY = "serverless.cross_project.enabled";
    private final boolean crossProjectEnabled;

    public CrossProjectModeDecider(Settings settings) {
        this.crossProjectEnabled = settings.getAsBoolean(CROSS_PROJECT_ENABLED_SETTING_KEY, false);
    }

    public boolean crossProjectEnabled() {
        return crossProjectEnabled;
    }

    public boolean resolvesCrossProject(IndicesRequest.CrossProjectCandidate request) {
        if (crossProjectEnabled == false) {
            return false;
        }
        if (request.allowsCrossProject() == false) {
            return false;
        }
        return request.resolvesCrossProject();
    }
}
