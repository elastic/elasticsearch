/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.search.crossproject.TargetProjects;

public interface CrossProjectSearchAuthorizationService {
    void loadAuthorizedProjects(ActionListener<TargetProjects> listener);

    class Default implements CrossProjectSearchAuthorizationService {
        @Override
        public void loadAuthorizedProjects(ActionListener<TargetProjects> listener) {
            listener.onResponse(TargetProjects.NOT_CROSS_PROJECT);
        }
    }
}
