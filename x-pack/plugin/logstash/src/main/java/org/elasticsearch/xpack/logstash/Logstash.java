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
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.template.TemplateUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

/**
 * This class activates/deactivates the logstash modules depending if we're running a node client or transport client
 */
public class Logstash extends Plugin implements ActionPlugin {

    private static final String LOGSTASH_TEMPLATE_NAME = "logstash-index-template";
    private static final String TEMPLATE_VERSION_PATTERN =
            Pattern.quote("${logstash.template.version}");

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
            TemplateUtils.loadTemplateIntoMap("/" + LOGSTASH_TEMPLATE_NAME + ".json", templates, LOGSTASH_TEMPLATE_NAME,
                    Version.CURRENT.toString(), TEMPLATE_VERSION_PATTERN, LogManager.getLogger(Logstash.class));
            return templates;
        };
    }
}
