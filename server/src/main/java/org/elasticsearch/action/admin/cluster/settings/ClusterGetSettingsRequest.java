/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;

/**
 * This request is specific to the REST client. {@link org.elasticsearch.action.admin.cluster.state.ClusterStateRequest}
 * is used on the transport layer.
 */
public class ClusterGetSettingsRequest extends MasterNodeReadRequest<ClusterGetSettingsRequest> {
    private boolean includeDefaults = false;

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * When include_defaults is set, return default settings which are normally suppressed.
     */
    public ClusterGetSettingsRequest includeDefaults(boolean includeDefaults) {
        this.includeDefaults = includeDefaults;
        return this;
    }

    public boolean includeDefaults() {
        return includeDefaults;
    }
}
