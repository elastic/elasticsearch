/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.TemplateBundle;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.watcher.support.WatcherIndexTemplateRegistryField;

import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.WATCHER_ORIGIN;
import static org.elasticsearch.xpack.core.template.IndexTemplateRegistry.parseComposableTemplates;

public class WatcherTemplateBundle implements TemplateBundle {

    public static final String WATCHER_TEMPLATE_VERSION_VARIABLE = "xpack.watcher.template.version";

    private static final Map<String, ComposableIndexTemplate> TEMPLATES_WATCH_HISTORY = parseComposableTemplates(
        new IndexTemplateConfig(
            WatcherIndexTemplateRegistryField.HISTORY_TEMPLATE_NAME,
            "/watch-history.json",
            WatcherIndexTemplateRegistryField.INDEX_TEMPLATE_VERSION,
            WATCHER_TEMPLATE_VERSION_VARIABLE
        )
    );

    private static final Map<String, ComposableIndexTemplate> TEMPLATES_WATCH_HISTORY_NO_ILM = parseComposableTemplates(
        new IndexTemplateConfig(
            WatcherIndexTemplateRegistryField.HISTORY_TEMPLATE_NAME_NO_ILM,
            "/watch-history-no-ilm.json",
            WatcherIndexTemplateRegistryField.INDEX_TEMPLATE_VERSION,
            WATCHER_TEMPLATE_VERSION_VARIABLE
        )
    );

    private final boolean ilmManagementEnabled;

    public WatcherTemplateBundle(Watcher watcher) {
        ilmManagementEnabled = Watcher.USE_ILM_INDEX_MANAGEMENT.get(watcher.settings);
    }

    @Override
    public Map<String, ComposableIndexTemplate> getComposableIndexTemplates() {
        return ilmManagementEnabled ? TEMPLATES_WATCH_HISTORY : TEMPLATES_WATCH_HISTORY_NO_ILM;
    }

    @Override
    public String getName() {
        return WATCHER_ORIGIN;
    }
}
