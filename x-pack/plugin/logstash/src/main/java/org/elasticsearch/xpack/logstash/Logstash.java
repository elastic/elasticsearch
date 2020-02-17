/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.logstash;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.template.TemplateUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * This class activates/deactivates the logstash modules depending if we're running a node client or transport client
 */
public class Logstash extends Plugin implements SystemIndexPlugin {

    private static final String LOGSTASH_CONCRETE_INDEX_NAME = ".logstash";
    private static final String LOGSTASH_TEMPLATE_FILE_NAME = "logstash-management";
    private static final String LOGSTASH_INDEX_TEMPLATE_NAME = ".logstash-management";
    private static final String OLD_LOGSTASH_INDEX_NAME = "logstash-index-template";
    private static final String TEMPLATE_VERSION_VARIABLE = "logstash.template.version";

    private final boolean enabled;
    private final boolean transportClientMode;

    public Logstash(Settings settings) {
        this.enabled = XPackSettings.LOGSTASH_ENABLED.get(settings);
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
    }

    boolean isEnabled() {
      return enabled;
    }

    boolean isTransportClient() {
      return transportClientMode;
    }

    public Collection<Module> createGuiceModules() {
        List<Module> modules = new ArrayList<>();
        modules.add(b -> {
            XPackPlugin.bindFeatureSet(b, LogstashFeatureSet.class);
        });
        return modules;
    }

    public UnaryOperator<Map<String, IndexTemplateMetaData>> getIndexTemplateMetaDataUpgrader() {
        return templates -> {
            templates.keySet().removeIf(OLD_LOGSTASH_INDEX_NAME::equals);
            TemplateUtils.loadTemplateIntoMap("/" + LOGSTASH_TEMPLATE_FILE_NAME + ".json", templates, LOGSTASH_INDEX_TEMPLATE_NAME,
                    Version.CURRENT.toString(), TEMPLATE_VERSION_VARIABLE, LogManager.getLogger(Logstash.class));
            //internal representation of typeless templates requires the default "_doc" type, which is also required for internal templates
            assert templates.get(LOGSTASH_INDEX_TEMPLATE_NAME).mappings().get(MapperService.SINGLE_MAPPING_NAME) != null;
            return templates;
        };
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors() {
        return Collections.singletonList(new SystemIndexDescriptor(LOGSTASH_CONCRETE_INDEX_NAME,
            "Contains data for Logstash Central Management"));
    }
}
