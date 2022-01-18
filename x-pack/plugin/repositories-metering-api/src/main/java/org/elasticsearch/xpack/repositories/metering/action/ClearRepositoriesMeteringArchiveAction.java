/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.repositories.metering.action;

import org.elasticsearch.action.ActionType;

public final class ClearRepositoriesMeteringArchiveAction extends ActionType<RepositoriesMeteringResponse> {
    public static final ClearRepositoriesMeteringArchiveAction INSTANCE = new ClearRepositoriesMeteringArchiveAction();

    static final String NAME = "cluster:monitor/xpack/repositories_metering/clear_metering_archive";

    ClearRepositoriesMeteringArchiveAction() {
        super(NAME, RepositoriesMeteringResponse::new);
    }
}
