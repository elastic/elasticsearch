/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.plugins.RepositoryPlugin;

import java.util.List;

public final class EncryptedRepositoryPlugin extends Plugin implements RepositoryPlugin, ReloadablePlugin {

    public EncryptedRepositoryPlugin(final Settings settings) {
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of();
    }

    @Override
    public void reload(Settings settings) {
        // Secure settings should be readable inside this method.
    }
}
