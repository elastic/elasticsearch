/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.search.lookup.LeafStoredFieldsLookup;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.transport.TransportService;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class CancellationIT extends ProfilingTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ScriptedBlockPlugin.class);
        return plugins;
    }

    @Override
    protected boolean useOnlyAllEvents() {
        // we assume that all indices have been created to simplify the testing logic.
        return false;
    }

    public void testAutomaticCancellation() throws Exception {
        Request restRequest = new Request("POST", "/_profiling/stacktraces");
        restRequest.setEntity(new StringEntity("""
                {
                  "sample_size": 10000,
                  "query": {
                    "bool": {
                      "filter": [
                        {
                          "script": {
                            "script": {
                              "lang": "mockscript",
                              "source": "search_block",
                              "params": {}
                            }
                          }
                        }
                      ]
                    }
                  }
                }
            """, ContentType.APPLICATION_JSON.withCharset(StandardCharsets.UTF_8)));
        verifyCancellation(GetProfilingAction.NAME, restRequest);
    }

    void verifyCancellation(String action, Request restRequest) throws Exception {
        Map<String, String> nodeIdToName = readNodesInfo();
        List<ScriptedBlockPlugin> plugins = initBlockFactory();

        PlainActionFuture<Response> future = PlainActionFuture.newFuture();
        Cancellable cancellable = getRestClient().performRequestAsync(restRequest, wrapAsRestResponseListener(future));

        awaitForBlock(plugins);
        Collection<TaskId> profilingTasks = collectProfilingRelatedTasks(action);
        cancellable.cancel();
        ensureTasksAreCancelled(profilingTasks, nodeIdToName::get);

        disableBlocks(plugins);
        expectThrows(CancellationException.class, future::actionGet);
    }

    private static Map<String, String> readNodesInfo() {
        Map<String, String> nodeIdToName = new HashMap<>();
        NodesInfoResponse nodesInfoResponse = clusterAdmin().prepareNodesInfo().get();
        assertFalse(nodesInfoResponse.hasFailures());
        for (NodeInfo node : nodesInfoResponse.getNodes()) {
            nodeIdToName.put(node.getNode().getId(), node.getNode().getName());
        }
        return nodeIdToName;
    }

    private static Collection<TaskId> collectProfilingRelatedTasks(String transportAction) {
        SetOnce<TaskInfo> profilingTask = new SetOnce<>();
        Map<TaskId, Set<TaskId>> taskToParent = new HashMap<>();
        ListTasksResponse listTasksResponse = clusterAdmin().prepareListTasks().get();
        for (TaskInfo task : listTasksResponse.getTasks()) {
            TaskId parentTaskId = task.parentTaskId();
            if (parentTaskId != null) {
                if (taskToParent.containsKey(parentTaskId) == false) {
                    taskToParent.put(parentTaskId, new HashSet<>());
                }
                taskToParent.get(parentTaskId).add(task.taskId());
            }
            if (task.action().equals(transportAction)) {
                profilingTask.set(task);
            }
        }
        assertNotNull(profilingTask.get());
        Set<TaskId> childTaskIds = taskToParent.get(profilingTask.get().taskId());
        Set<TaskId> profilingTaskIds = new HashSet<>();
        profilingTaskIds.add(profilingTask.get().taskId());
        if (childTaskIds != null) {
            profilingTaskIds.addAll(childTaskIds);
        }
        return profilingTaskIds;
    }

    private static void ensureTasksAreCancelled(Collection<TaskId> taskIds, Function<String, String> nodeIdToName) throws Exception {
        assertBusy(() -> {
            for (TaskId taskId : taskIds) {
                String nodeName = nodeIdToName.apply(taskId.getNodeId());
                TaskManager taskManager = internalCluster().getInstance(TransportService.class, nodeName).getTaskManager();
                Task task = taskManager.getTask(taskId.getId());
                // as we capture the task hierarchy at the beginning but cancel in the middle of execution, some tasks have been
                // unregistered already by the time we verify cancellation.
                if (task != null) {
                    assertThat(task, instanceOf(CancellableTask.class));
                    assertTrue(((CancellableTask) task).isCancelled());
                }
            }
        });
    }

    private static List<ScriptedBlockPlugin> initBlockFactory() {
        List<ScriptedBlockPlugin> plugins = new ArrayList<>();
        for (PluginsService pluginsService : internalCluster().getDataNodeInstances(PluginsService.class)) {
            plugins.addAll(pluginsService.filterPlugins(ScriptedBlockPlugin.class));
        }
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.reset();
            plugin.enableBlock();
            // Allow to execute one search and only block starting with the second one. This
            // is done so we have at least one child action and can check that all active children
            // are cancelled with the parent action.
            plugin.setSlack(1);
        }
        return plugins;
    }

    private void awaitForBlock(List<ScriptedBlockPlugin> plugins) throws Exception {
        assertBusy(() -> {
            int numberOfBlockedPlugins = 0;
            for (ScriptedBlockPlugin plugin : plugins) {
                numberOfBlockedPlugins += plugin.hits.get();
            }
            logger.info("The plugin blocked on {} shards", numberOfBlockedPlugins);
            assertThat(numberOfBlockedPlugins, greaterThan(0));
        }, 10, TimeUnit.SECONDS);
    }

    private static void disableBlocks(List<ScriptedBlockPlugin> plugins) {
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.disableBlock();
        }
    }

    public static class ScriptedBlockPlugin extends MockScriptPlugin {
        static final String SCRIPT_NAME = "search_block";

        private final AtomicInteger hits = new AtomicInteger();

        private final AtomicInteger slack = new AtomicInteger(0);

        private final AtomicBoolean shouldBlock = new AtomicBoolean(true);

        void reset() {
            hits.set(0);
        }

        void disableBlock() {
            shouldBlock.set(false);
        }

        void enableBlock() {
            shouldBlock.set(true);
        }

        void setSlack(int slack) {
            this.slack.set(slack);
        }

        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(SCRIPT_NAME, params -> {
                LeafStoredFieldsLookup fieldsLookup = (LeafStoredFieldsLookup) params.get("_fields");
                LogManager.getLogger(CancellationIT.class).info("Blocking on the document {}", fieldsLookup.get("_id"));
                hits.incrementAndGet();
                if (slack.decrementAndGet() < 0) {
                    try {
                        waitUntil(() -> shouldBlock.get() == false);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return true;
            });
        }
    }
}
