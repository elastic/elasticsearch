/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.useragent;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.UserAgentParserRegistryProvider;

import java.nio.file.Path;
import java.util.List;

public class UserAgentPlugin extends Plugin implements UserAgentParserRegistryProvider {

    private final Setting<Long> chacheSizeSetting;

    public UserAgentPlugin() {
        Setting<Long> deprecatedCacheSizeSetting = Setting.longSetting("ingest.user_agent.cache_size", 1000, 0, Setting.Property.NodeScope);
        chacheSizeSetting = Setting.longSetting("user_agent.cache_size", deprecatedCacheSizeSetting, 0, Setting.Property.NodeScope);
    }

    @Override
    public org.elasticsearch.useragent.api.UserAgentParserRegistry createUserAgentParserRegistry(Environment env) {
        return createRegistry(env, env.settings());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(chacheSizeSetting);
    }

    /**
     * Public static factory for creating a {@link UserAgentParserRegistry}.
     * Used by the logstash-bridge to create a registry without the plugin system.
     */
    public static UserAgentParserRegistry createRegistry(Environment env, Settings settings) {
        Setting<Long> deprecatedCacheSizeSetting = Setting.longSetting("ingest.user_agent.cache_size", 1000, 0, Setting.Property.NodeScope);
        Setting<Long> cacheSizeSetting = Setting.longSetting("user_agent.cache_size", deprecatedCacheSizeSetting, 0,
            Setting.Property.NodeScope);
        Path userAgentConfigDirectory = env.configDir().resolve("user-agent");
        Path ingestUserAgentConfigDirectory = env.configDir().resolve("ingest-user-agent");
        long cacheSize = cacheSizeSetting.get(settings);
        UserAgentCache cache = new UserAgentCache(cacheSize);
        return new UserAgentParserRegistry(cache, userAgentConfigDirectory, ingestUserAgentConfigDirectory);
    }
}
