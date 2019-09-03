/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
