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

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class TemplateUpgradeServiceIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestPlugin.class);
    }

    public static class TestPlugin extends Plugin {
        static final Setting<Boolean> UPDATE_TEMPLATE_ENABLED =
            Setting.boolSetting("tests.update_template_enabled", false, Setting.Property.NodeScope);

        protected final Logger logger;
        protected final Settings settings;

        public TestPlugin(Settings settings) {
            this.logger = Loggers.getLogger(getClass(), settings);
            this.settings = settings;
        }

        @Override
        public UnaryOperator<Map<String, IndexTemplateMetaData>> getIndexTemplateMetaDataUpgrader() {
            return templates -> {
                if (UPDATE_TEMPLATE_ENABLED.get(settings)) {
                    templates.put("test_added_template", IndexTemplateMetaData.builder("test_added_template")
                        .patterns(Collections.singletonList("*")).build());
                    templates.remove("test_removed_template");
                    templates.put("test_changed_template", IndexTemplateMetaData.builder("test_changed_template").order(10)
                        .patterns(Collections.singletonList("*")).build());
                }
                return templates;
            };
        }

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(UPDATE_TEMPLATE_ENABLED);
        }
    }


    public void testTemplateUpdate() throws Exception {
        assertThat(client().admin().indices().prepareGetTemplates("test_*").get().getIndexTemplates(), empty());
        assertAcked(client().admin().indices().preparePutTemplate("test_dummy_template").setOrder(0)
            .setPatterns(Collections.singletonList("*")).get());
        assertAcked(client().admin().indices().preparePutTemplate("test_changed_template").setOrder(0)
            .setPatterns(Collections.singletonList("*")).get());
        assertAcked(client().admin().indices().preparePutTemplate("test_removed_template").setOrder(1)
            .setPatterns(Collections.singletonList("*")).get());
        String upgradeNode = internalCluster().startNode(Settings.builder()
            .put(nodeSettings(0)).put(TestPlugin.UPDATE_TEMPLATE_ENABLED.getKey(), true));
        // Waiting for the templates to be updated by the newly added node

        assertBusy(() -> {
            List<IndexTemplateMetaData> templates = client().admin().indices().prepareGetTemplates("test_*").get().getIndexTemplates();
            assertThat(templates.size(), equalTo(3));
            boolean addedFound = false;
            boolean changedFound = false;
            boolean dummyFound = false;
            for (int i = 0; i < 3; i++) {
                IndexTemplateMetaData templateMetaData = templates.get(i);
                switch (templateMetaData.getName()) {
                    case "test_added_template":
                        assertFalse(addedFound);
                        addedFound = true;
                        break;
                    case "test_changed_template":
                        assertFalse(changedFound);
                        changedFound = true;
                        assertThat(templateMetaData.getOrder(), equalTo(10));
                        break;
                    case "test_dummy_template":
                        assertFalse(dummyFound);
                        dummyFound = true;
                        break;
                    default:
                        fail("unexpected template " + templateMetaData.getName());
                        break;
                }
            }

            assertTrue(addedFound);
            assertTrue(changedFound);
            assertTrue(dummyFound);
        });

        // Wait for upgradeNode to finish all updates
        TemplateUpgradeService service = internalCluster().getInstance(TemplateUpgradeService.class, upgradeNode);
        assertBusy(() -> assertFalse(service.hasUpdatesInProgress()));

        // Wipe out all templates
        assertAcked(client().admin().indices().prepareDeleteTemplate("test_*").get());

        // Make sure all templates are recreated correctly
        assertBusy(() -> {
            List<IndexTemplateMetaData> templates = client().admin().indices().prepareGetTemplates("test_*").get().getIndexTemplates();
            assertThat(templates.size(), equalTo(2));
            boolean addedFound = false;
            boolean changedFound = false;
            for (int i = 0; i < 2; i++) {
                IndexTemplateMetaData templateMetaData = templates.get(i);
                switch (templateMetaData.getName()) {
                    case "test_added_template":
                        assertFalse(addedFound);
                        addedFound = true;
                        break;
                    case "test_changed_template":
                        assertFalse(changedFound);
                        changedFound = true;
                        assertThat(templateMetaData.getOrder(), equalTo(10));
                        break;
                    default:
                        fail("unexpected template " + templateMetaData.getName());
                        break;
                }
            }

            assertTrue(addedFound);
            assertTrue(changedFound);
        });
    }

}
