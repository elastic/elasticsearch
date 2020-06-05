/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.logstash;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.template.TemplateUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * This class supplies the logstash featureset and templates
 */
public class Logstash extends Plugin implements SystemIndexPlugin {

    private static final String LOGSTASH_CONCRETE_INDEX_NAME = ".logstash";
    private static final String LOGSTASH_TEMPLATE_FILE_NAME = "logstash-management";
    private static final String LOGSTASH_INDEX_TEMPLATE_NAME = ".logstash-management";
    private static final String OLD_LOGSTASH_INDEX_NAME = "logstash-index-template";
    private static final String TEMPLATE_VERSION_VARIABLE = "logstash.template.version";

    public Logstash() {}

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(XPackUsageFeatureAction.LOGSTASH, LogstashUsageTransportAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.LOGSTASH, LogstashInfoTransportAction.class)
        );
    }

    public UnaryOperator<Map<String, IndexTemplateMetadata>> getIndexTemplateMetadataUpgrader() {
        return templates -> {
            templates.keySet().removeIf(OLD_LOGSTASH_INDEX_NAME::equals);
            TemplateUtils.loadTemplateIntoMap(
                "/" + LOGSTASH_TEMPLATE_FILE_NAME + ".json",
                templates,
                LOGSTASH_INDEX_TEMPLATE_NAME,
                Version.CURRENT.toString(),
                TEMPLATE_VERSION_VARIABLE,
                LogManager.getLogger(Logstash.class)
            );
            assert templates.get(LOGSTASH_INDEX_TEMPLATE_NAME).mappings() != null;
            return templates;
        };
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return Collections.singletonList(
            new SystemIndexDescriptor(LOGSTASH_CONCRETE_INDEX_NAME, "Contains data for Logstash Central Management")
        );
    }
}
