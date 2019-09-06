/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;

public class EncryptedRepositoryPlugin extends Plugin implements RepositoryPlugin, ReloadablePlugin {

    private final Repository.Factory encryptedRepositoryFactory;

    public EncryptedRepositoryPlugin(final Settings settings) {
        encryptedRepositoryFactory = EncryptedRepository.newRepositoryFactory(settings);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry,
                                                           ThreadPool threadPool) {
        return Map.of("encrypted", encryptedRepositoryFactory);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(EncryptedRepository.ENCRYPTION_PASSWORD_SETTING);
    }

    @Override
    public void reload(Settings settings) {
        // Secure settings should be readable inside this method.
    }
}
