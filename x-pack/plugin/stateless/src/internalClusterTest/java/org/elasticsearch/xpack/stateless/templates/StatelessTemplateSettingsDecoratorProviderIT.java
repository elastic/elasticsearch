/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.templates;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.Plugin.PluginServices;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.util.CollectionUtils.appendToCopy;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

public class StatelessTemplateSettingsDecoratorProviderIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return appendToCopy(super.nodePlugins(), PluginWithTemplate.class);
    }

    public void testCreateStatelessCluster() throws Exception {
        startMasterOnlyNode();

        assertBusy(() -> {
            var metadata = clusterService().state().projectState(Metadata.DEFAULT_PROJECT_ID).metadata();
            var templates = metadata.templatesV2();
            assertThat(
                templates,
                // assert that all ILM lifecycle settings are removed
                hasEntry(is("decorated-template"), transformedMatch("settings", ct -> ct.template().settings(), equalTo(Settings.EMPTY)))
            );
        });
    }

    public static class PluginWithTemplate extends Plugin {
        @Override
        public Collection<?> createComponents(PluginServices services) {
            new TemplateRegistry(services).initialize();
            return List.of();
        }
    }

    static class TemplateRegistry extends IndexTemplateRegistry {

        private Map<String, ComposableIndexTemplate> TEMPLATES = parseComposableTemplates(
            new IndexTemplateConfig("decorated-template", "/template-with-ilm.json", 1, "version")
        );

        TemplateRegistry(PluginServices services) {
            super(
                services.environment().settings(),
                services.clusterService(),
                services.threadPool(),
                services.client(),
                services.xContentRegistry()
            );
        }

        @Override
        protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
            return TEMPLATES;
        }

        @Override
        protected String getOrigin() {
            return "test";
        }
    }
}
