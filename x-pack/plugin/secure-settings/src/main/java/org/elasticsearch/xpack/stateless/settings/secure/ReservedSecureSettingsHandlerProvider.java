/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.ReservedProjectStateHandler;
import org.elasticsearch.reservedstate.ReservedStateHandlerProvider;

import java.util.Collection;
import java.util.List;

public class ReservedSecureSettingsHandlerProvider implements ReservedStateHandlerProvider {
    @Override
    public Collection<ReservedClusterStateHandler<?>> clusterHandlers() {
        return List.of(new ReservedClusterSecretsAction());
    }

    @Override
    public Collection<ReservedProjectStateHandler<?>> projectHandlers() {
        return List.of(new ReservedProjectSecretsAction());
    }
}
