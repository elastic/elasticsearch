/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.repositories.cleanup;

import org.elasticsearch.action.ActionType;

public final class CleanupRepositoryAction extends ActionType<CleanupRepositoryResponse> {

    public static final CleanupRepositoryAction INSTANCE = new CleanupRepositoryAction();
    public static final String NAME = "cluster:admin/repository/_cleanup";

    private CleanupRepositoryAction() {
        super(NAME, CleanupRepositoryResponse::new);
    }
}
