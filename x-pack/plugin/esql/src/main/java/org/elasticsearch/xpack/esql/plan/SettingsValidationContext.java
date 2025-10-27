/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.Build;
import org.elasticsearch.transport.RemoteClusterService;

public class SettingsValidationContext {

    private final RemoteClusterService remoteClusterService;

    public SettingsValidationContext(RemoteClusterService remoteClusterService) {
        this.remoteClusterService = remoteClusterService;
    }

    public boolean crossProjectEnabled() {
        return remoteClusterService.crossProjectEnabled();
    }

    public boolean isSnapshot() {
        return Build.current().isSnapshot();
    }
}
