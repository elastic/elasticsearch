/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.repositories.metering;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.repositories.metering.action.ClearRepositoriesMeteringArchiveAction;
import org.elasticsearch.xpack.repositories.metering.action.RepositoriesMeteringAction;
import org.elasticsearch.xpack.repositories.metering.action.TransportClearRepositoriesStatsArchiveAction;
import org.elasticsearch.xpack.repositories.metering.action.TransportRepositoriesStatsAction;
import org.elasticsearch.xpack.repositories.metering.rest.RestClearRepositoriesMeteringArchiveAction;
import org.elasticsearch.xpack.repositories.metering.rest.RestGetRepositoriesMeteringAction;

import java.util.List;

public final class RepositoriesMeteringPlugin extends Plugin implements ActionPlugin {

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(RepositoriesMeteringAction.INSTANCE, TransportRepositoriesStatsAction.class),
            new ActionHandler<>(ClearRepositoriesMeteringArchiveAction.INSTANCE, TransportClearRepositoriesStatsArchiveAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(RestHandlerParameters parameters) {
        return List.of(new RestGetRepositoriesMeteringAction(), new RestClearRepositoriesMeteringArchiveAction());
    }
}
