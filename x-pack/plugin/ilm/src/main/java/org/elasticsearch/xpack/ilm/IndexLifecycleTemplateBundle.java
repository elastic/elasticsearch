/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.TemplateBundle;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;

import java.util.Map;

import static org.elasticsearch.xpack.core.template.IndexTemplateRegistry.parseComposableTemplates;

public class IndexLifecycleTemplateBundle implements TemplateBundle {

    // history (please add a comment why you increased the version here)
    // version 1: initial
    // version 2: convert to hidden index
    // version 3: templates moved to composable templates
    // version 4: add `allow_auto_create` setting
    // version 5: convert to data stream
    public static final int INDEX_TEMPLATE_VERSION = 5;

    public static final String ILM_TEMPLATE_VERSION_VARIABLE = "xpack.ilm_history.template.version";
    public static final String ILM_TEMPLATE_NAME = "ilm-history";

    public static final String SLM_TEMPLATE_VERSION_VARIABLE = "xpack.slm.template.version";
    public static final String SLM_TEMPLATE_NAME = ".slm-history";

    public static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATE_CONFIGS = parseComposableTemplates(
        new IndexTemplateConfig(ILM_TEMPLATE_NAME, "/ilm-history.json", INDEX_TEMPLATE_VERSION, ILM_TEMPLATE_VERSION_VARIABLE),
        new IndexTemplateConfig(SLM_TEMPLATE_NAME, "/slm-history.json", INDEX_TEMPLATE_VERSION, SLM_TEMPLATE_VERSION_VARIABLE)
    );

    private final boolean ilmHistoryEnabled;

    public IndexLifecycleTemplateBundle(IndexLifecycle plugin) {
        ilmHistoryEnabled = LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING.get(plugin.settings);
    }

    @Override
    public Map<String, ComposableIndexTemplate> getComposableIndexTemplates() {
        return ilmHistoryEnabled ? COMPOSABLE_INDEX_TEMPLATE_CONFIGS : Map.of();
    }

    @Override
    public String getName() {
        return ClientHelper.INDEX_LIFECYCLE_ORIGIN;
    }

}
