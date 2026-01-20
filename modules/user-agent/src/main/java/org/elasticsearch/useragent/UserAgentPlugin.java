/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.useragent;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.env.Environment;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

public class UserAgentPlugin extends Plugin implements IngestPlugin {

    private final SetOnce<UserAgentParserRegistry> userAgentParserRegistry = new SetOnce<>();
    private final Setting<Long> chacheSizeSetting;

    public UserAgentPlugin() {
        Setting<Long> deprecatedCacheSizeSetting = Setting.longSetting("ingest.user_agent.cache_size", 1000, 0, Setting.Property.NodeScope);
        chacheSizeSetting = Setting.longSetting("user_agent.cache_size", deprecatedCacheSizeSetting, 0, Setting.Property.NodeScope);
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        return List.of(createAndGetRegistry(services.environment()));
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        UserAgentParserRegistry userAgentParserRegistry = createAndGetRegistry(parameters.env);
        return Map.of(UserAgentProcessor.TYPE, new UserAgentProcessor.Factory(userAgentParserRegistry));
    }

    private UserAgentParserRegistry createAndGetRegistry(Environment env) {
        UserAgentParserRegistry ret = userAgentParserRegistry.get();
        if (ret != null) {
            return ret;
        }
        Path userAgentConfigDirectory = env.configDir().resolve("user-agent");
        Path ingestUserAgentConfigDirectory = env.configDir().resolve("ingest-user-agent");
        long cacheSize = chacheSizeSetting.get(env.settings());
        UserAgentCache cache = new UserAgentCache(cacheSize);
        UserAgentParserRegistry registry = new UserAgentParserRegistry(cache, userAgentConfigDirectory, ingestUserAgentConfigDirectory);
        userAgentParserRegistry.set(registry);
        return registry;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(chacheSizeSetting);
    }

    @Override
    public Map<String, UnaryOperator<Metadata.ProjectCustom>> getProjectCustomMetadataUpgraders() {
        return Map.of(
            IngestMetadata.TYPE,
            ingestMetadata -> ((IngestMetadata) ingestMetadata).maybeUpgradeProcessors(
                UserAgentProcessor.TYPE,
                UserAgentProcessor::maybeUpgradeConfig
            )
        );
    }
}
