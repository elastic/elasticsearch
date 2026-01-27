/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.plugin.security.cloudiam;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHeaderDefinition;

import java.util.Collection;
import java.util.List;

public class CloudIamRealmPlugin extends Plugin implements ActionPlugin {
    private static final String DEFAULT_AUTH_HEADER = "X-ES-IAM-Auth";
    private static final String DEFAULT_SIGNED_HEADER = "X-ES-IAM-Signed";

    @Override
    public List<Setting<?>> getSettings() {
        return CloudIamRealmSettings.getSettings();
    }

    @Override
    public Collection<RestHeaderDefinition> getRestHeaders() {
        return List.of(
            new RestHeaderDefinition(DEFAULT_AUTH_HEADER, false),
            new RestHeaderDefinition(DEFAULT_SIGNED_HEADER, false)
        );
    }
}
