/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm.history;

import org.elasticsearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.ilm.IndexLifecycle;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.ilm.history.ILMHistoryTemplateRegistry.ILM_TEMPLATE_NAME;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ILMHistoryTemplateRegistryIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateCompositeXPackPlugin.class, DataStreamsPlugin.class, IndexLifecycle.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED, false)
            .build();
    }

    public void testTemplateInstalledWhenSettingEnabled() throws Exception {
        // With the setting disabled at startup, the template should not be present.
        GetComposableIndexTemplateAction.Response response = client().execute(
            GetComposableIndexTemplateAction.INSTANCE,
            new GetComposableIndexTemplateAction.Request(TEST_REQUEST_TIMEOUT, "*")
        ).get();
        assertThat(response.indexTemplates(), not(hasKey(ILM_TEMPLATE_NAME)));

        // Dynamically enable ILM history; the registry should now install the template.
        updateClusterSettings(Settings.builder().put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED, true));

        assertBusy(() -> {
            GetComposableIndexTemplateAction.Response r = client().execute(
                GetComposableIndexTemplateAction.INSTANCE,
                new GetComposableIndexTemplateAction.Request(TEST_REQUEST_TIMEOUT, "*")
            ).get();
            assertThat(r.indexTemplates(), hasKey(ILM_TEMPLATE_NAME));
        }, 10, TimeUnit.SECONDS);
    }
}
