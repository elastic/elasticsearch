/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.search.crossproject.TargetProjects;

/**
 * A supplier of authorized projects for the current user. This includes the origin project and all linked projects the user has access to.
 * If we are not in a cross-project search context, the supplier returns {@link TargetProjects#NOT_CROSS_PROJECT}.
 */
public interface AuthorizedProjectsSupplier {
    void getAuthorizedProjects(ActionListener<TargetProjects> listener);

    boolean recordResolvedIndexExpressions();

    class Default implements AuthorizedProjectsSupplier {
        @Override
        public void getAuthorizedProjects(ActionListener<TargetProjects> listener) {
            listener.onResponse(TargetProjects.NOT_CROSS_PROJECT);
        }

        @Override
        public boolean recordResolvedIndexExpressions() {
            return false;
        }
    }
}
