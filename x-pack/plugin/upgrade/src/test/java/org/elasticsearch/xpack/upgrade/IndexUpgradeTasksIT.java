/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TaskGroup;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.protocol.xpack.migration.IndexUpgradeInfoResponse;
import org.elasticsearch.protocol.xpack.migration.UpgradeActionRequired;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.upgrade.UpgradeField;
import org.elasticsearch.xpack.core.upgrade.actions.IndexUpgradeAction;
import org.elasticsearch.xpack.core.upgrade.actions.IndexUpgradeInfoAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = TEST, supportsDedicatedMasters = false, numClientNodes = 0, maxNumDataNodes = 1)
public class IndexUpgradeTasksIT extends ESIntegTestCase {

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockUpgradePlugin.class, ReindexPlugin.class);
    }

    public static class MockUpgradePlugin extends Plugin implements ScriptPlugin, ActionPlugin {

        public static final String NAME = MockScriptEngine.NAME;

        private Settings settings;
        private Upgrade upgrade;

        private CountDownLatch upgradeLatch = new CountDownLatch(1);
        private CountDownLatch upgradeCalledLatch = new CountDownLatch(1);

        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new MockScriptEngine(pluginScriptLang(), pluginScripts(), Collections.emptyMap());
        }

        public String pluginScriptLang() {
            return NAME;
        }

        public MockUpgradePlugin(Settings settings) {
            this.settings = settings;
            this.upgrade = new Upgrade();
            LogManager.getLogger(IndexUpgradeTasksIT.class).info("MockUpgradePlugin is created");
        }


        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
            scripts.put("block", map -> {
                upgradeCalledLatch.countDown();
                try {
                    assertThat(upgradeLatch.await(10, TimeUnit.SECONDS), equalTo(true));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return null;
            });
            return scripts;
        }

        @Override
        public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                                   ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                                   NamedXContentRegistry xContentRegistry, Environment environment,
                                                   NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
            return Arrays.asList(new IndexUpgradeService(Collections.singletonList(
                    new IndexUpgradeCheck("test",
                            new Function<IndexMetaData, UpgradeActionRequired>() {
                                @Override
                                public UpgradeActionRequired apply(IndexMetaData indexMetaData) {
                                    if ("test".equals(indexMetaData.getIndex().getName())) {
                                        if (UpgradeField.checkInternalIndexFormat(indexMetaData)) {
                                            return UpgradeActionRequired.UP_TO_DATE;
                                        } else {
                                            return UpgradeActionRequired.UPGRADE;
                                        }
                                    } else {
                                        return UpgradeActionRequired.NOT_APPLICABLE;
                                    }
                                }
                            },
                            client, clusterService, Strings.EMPTY_ARRAY,
                            new Script(ScriptType.INLINE, NAME, "block", Collections.emptyMap()))
            )), new XPackLicenseState(settings));
        }

        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return upgrade.getActions();
        }

        @Override
        public Collection<String> getRestHeaders() {
            return upgrade.getRestHeaders();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    public void testParentTasksDuringUpgrade() throws Exception {
        logger.info("before getInstance");
        PluginsService pluginsService = internalCluster().getDataNodeInstance(PluginsService.class);
        MockUpgradePlugin mockUpgradePlugin = pluginsService.filterPlugins(MockUpgradePlugin.class).get(0);
        assertThat(mockUpgradePlugin, notNullValue());
        logger.info("after getInstance");

        assertAcked(client().admin().indices().prepareCreate("test").get());
        client().prepareIndex("test", "doc", "1").setSource("{\"foo\": \"bar\"}", XContentType.JSON)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        ensureYellow("test");


        IndexUpgradeInfoResponse infoResponse = new IndexUpgradeInfoAction.RequestBuilder(client()).setIndices("test").get();
        assertThat(infoResponse.getActions().keySet(), contains("test"));
        assertThat(infoResponse.getActions().get("test"), equalTo(UpgradeActionRequired.UPGRADE));


        ActionFuture<BulkByScrollResponse> upgradeResponse = new IndexUpgradeAction.RequestBuilder(client()).setIndex("test").execute();


        assertThat(mockUpgradePlugin.upgradeCalledLatch.await(10, TimeUnit.SECONDS), equalTo(true));
        ListTasksResponse response = client().admin().cluster().prepareListTasks().get();
        mockUpgradePlugin.upgradeLatch.countDown();

        // Find the upgrade task group
        TaskGroup upgradeGroup = null;
        for (TaskGroup group : response.getTaskGroups()) {
            if (IndexUpgradeAction.NAME.equals(group.getTaskInfo().getAction())) {
                assertThat(upgradeGroup, nullValue());
                upgradeGroup = group;
            }
        }
        assertThat(upgradeGroup, notNullValue());
        assertThat(upgradeGroup.getTaskInfo().isCancellable(), equalTo(true)); // The task should be cancellable
        assertThat(upgradeGroup.getChildTasks(), hasSize(1)); // The reindex task should be a child
        assertThat(upgradeGroup.getChildTasks().get(0).getTaskInfo().getAction(), equalTo(ReindexAction.NAME));

        assertThat(upgradeResponse.get().getCreated(), equalTo(1L));
    }
}
