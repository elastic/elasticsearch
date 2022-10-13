/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class TemplateUpgradeServiceIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestPlugin.class);
    }

    public static final class TestPlugin extends Plugin {
        // This setting is used to simulate cluster state updates
        static final Setting<Integer> UPDATE_TEMPLATE_DUMMY_SETTING = Setting.intSetting(
            "tests.update_template_count",
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );
        private static final Logger logger = LogManager.getLogger(TestPlugin.class);

        protected final Settings settings;

        public TestPlugin(Settings settings) {
            this.settings = settings;
        }

        @Override
        public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver expressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier,
            Tracer tracer,
            AllocationDeciders allocationDeciders
        ) {
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(
                    UPDATE_TEMPLATE_DUMMY_SETTING,
                    integer -> { logger.debug("the template dummy setting was updated to {}", integer); }
                );
            return super.createComponents(
                client,
                clusterService,
                threadPool,
                resourceWatcherService,
                scriptService,
                xContentRegistry,
                environment,
                nodeEnvironment,
                namedWriteableRegistry,
                expressionResolver,
                repositoriesServiceSupplier,
                tracer,
                allocationDeciders
            );
        }

        @Override
        public UnaryOperator<Map<String, IndexTemplateMetadata>> getIndexTemplateMetadataUpgrader() {
            return templates -> {
                templates.put(
                    "test_added_template",
                    IndexTemplateMetadata.builder("test_added_template").patterns(Collections.singletonList("*")).build()
                );
                templates.remove("test_removed_template");
                templates.put(
                    "test_changed_template",
                    IndexTemplateMetadata.builder("test_changed_template").order(10).patterns(Collections.singletonList("*")).build()
                );
                return templates;
            };
        }

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(UPDATE_TEMPLATE_DUMMY_SETTING);
        }
    }

    public void testTemplateUpdate() throws Exception {
        assertTemplates();

        // Change some templates
        assertAcked(
            client().admin()
                .indices()
                .preparePutTemplate("test_dummy_template")
                .setOrder(0)
                .setPatterns(Collections.singletonList("*"))
                .get()
        );
        assertAcked(
            client().admin()
                .indices()
                .preparePutTemplate("test_changed_template")
                .setOrder(0)
                .setPatterns(Collections.singletonList("*"))
                .get()
        );
        assertAcked(
            client().admin()
                .indices()
                .preparePutTemplate("test_removed_template")
                .setOrder(1)
                .setPatterns(Collections.singletonList("*"))
                .get()
        );

        AtomicInteger updateCount = new AtomicInteger();
        // Wait for the templates to be updated back to normal
        assertBusy(() -> {
            // the updates only happen on cluster state updates, so we need to make sure that the cluster state updates are happening
            // so we need to simulate updates to make sure the template upgrade kicks in
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(
                        Settings.builder().put(TestPlugin.UPDATE_TEMPLATE_DUMMY_SETTING.getKey(), updateCount.incrementAndGet())
                    )
                    .get()
            );
            List<IndexTemplateMetadata> templates = client().admin().indices().prepareGetTemplates("test_*").get().getIndexTemplates();
            assertThat(templates, hasSize(3));
            boolean addedFound = false;
            boolean changedFound = false;
            boolean dummyFound = false;
            for (int i = 0; i < 3; i++) {
                IndexTemplateMetadata templateMetadata = templates.get(i);
                switch (templateMetadata.getName()) {
                    case "test_added_template" -> {
                        assertFalse(addedFound);
                        addedFound = true;
                    }
                    case "test_changed_template" -> {
                        assertFalse(changedFound);
                        changedFound = true;
                        assertThat(templateMetadata.getOrder(), equalTo(10));
                    }
                    case "test_dummy_template" -> {
                        assertFalse(dummyFound);
                        dummyFound = true;
                    }
                    default -> fail("unexpected template " + templateMetadata.getName());
                }
            }
            assertTrue(addedFound);
            assertTrue(changedFound);
            assertTrue(dummyFound);
        });

        // Wipe out all templates
        assertAcked(client().admin().indices().prepareDeleteTemplate("test_*").get());

        assertTemplates();

    }

    private void assertTemplates() throws Exception {
        AtomicInteger updateCount = new AtomicInteger();
        // Make sure all templates are recreated correctly
        assertBusy(() -> {
            // the updates only happen on cluster state updates, so we need to make sure that the cluster state updates are happening
            // so we need to simulate updates to make sure the template upgrade kicks in
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(
                        Settings.builder().put(TestPlugin.UPDATE_TEMPLATE_DUMMY_SETTING.getKey(), updateCount.incrementAndGet())
                    )
                    .get()
            );

            List<IndexTemplateMetadata> templates = client().admin().indices().prepareGetTemplates("test_*").get().getIndexTemplates();
            assertThat(templates, hasSize(2));
            boolean addedFound = false;
            boolean changedFound = false;
            for (int i = 0; i < 2; i++) {
                IndexTemplateMetadata templateMetadata = templates.get(i);
                switch (templateMetadata.getName()) {
                    case "test_added_template" -> {
                        assertFalse(addedFound);
                        addedFound = true;
                    }
                    case "test_changed_template" -> {
                        assertFalse(changedFound);
                        changedFound = true;
                        assertThat(templateMetadata.getOrder(), equalTo(10));
                    }
                    default -> fail("unexpected template " + templateMetadata.getName());
                }
            }

            assertTrue(addedFound);
            assertTrue(changedFound);
        });
    }

}
