/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.tasks;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.cluster.RemoteConnectionInfo;
import org.elasticsearch.client.cluster.RemoteInfoRequest;
import org.elasticsearch.client.cluster.RemoteInfoResponse;
import org.elasticsearch.client.tasks.CancelTasksRequest;
import org.elasticsearch.client.tasks.TaskId;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class TaskCancellationIT extends ESRestTestCase {
    private RestHighLevelClient oldCluster;
    private RestHighLevelClient newCluster;

    private List<HttpHost> readHosts(String prop) {
        final String address = System.getProperty(prop);
        assertNotNull("[" + prop + "] is not configured", address);
        List<HttpHost> hosts = parseHosts(address);
        assertThat("[" + prop + "] is empty", hosts, not(empty()));
        return hosts;
    }

    void configureRemoteCluster(RestHighLevelClient client, String remoteCluster, List<HttpHost> seedNodes) throws Exception {
        final String seeds = seedNodes.stream().map(HttpHost::toHostString).collect(Collectors.joining(","));
        logger.info("Configure remote cluster [{}] with seed [{}]", remoteCluster, seeds);
        final Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"persistent\": {\"cluster.remote." + remoteCluster + ".seeds\": \"" + seeds + "\"}}");
        client.cluster().putSettings(new ClusterUpdateSettingsRequest()
                .persistentSettings(Settings.builder().put("cluster.remote." + remoteCluster + ".seeds", seeds)),
            RequestOptions.DEFAULT);
        assertBusy(() -> {
            final RemoteInfoResponse remoteInfo = client.cluster().remoteInfo(new RemoteInfoRequest(), RequestOptions.DEFAULT);
            assertThat(remoteInfo.getInfos(), not(empty()));
            for (RemoteConnectionInfo info : remoteInfo.getInfos()) {
                assertThat(info.getClusterAlias(), equalTo(remoteCluster));
                assertTrue(info.isConnected());
            }
        });
    }

    Set<String> getNodeIds(RestHighLevelClient client) throws IOException {
        Response response = client.getLowLevelClient().performRequest(new Request("GET", "_nodes"));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final Map<String, Object> nodeMap = objectPath.evaluate("nodes");
        return nodeMap.keySet();
    }

    @Before
    public void initClientsAndClusters() throws Exception {
        RestClientBuilder.RequestConfigCallback callback = config -> config.setSocketTimeout(120 * 1000);
        oldCluster = new RestHighLevelClient(
            RestClient.builder(readHosts("tests.rest.old_cluster").toArray(new HttpHost[0])).setRequestConfigCallback(callback));
        newCluster = new RestHighLevelClient(
            RestClient.builder(readHosts("tests.rest.cluster").toArray(new HttpHost[0])).setRequestConfigCallback(callback));
        // connect the new cluster to the old one
        configureRemoteCluster(newCluster, "old", randomSubsetOf(1, readHosts("tests.old_cluster")));
    }

    @After
    public void destroyClients() throws IOException {
        IOUtils.close(oldCluster, newCluster);
    }

    public void testCancelTasks() throws Exception {
        Collection<String> newNodes = randomSubsetOf(getNodeIds(newCluster));
        Collection<String> oldNodes = randomSubsetOf(between(1, 2), getNodeIds(oldCluster));
        Thread thread = new Thread(() -> {
            final ResponseException exception = expectThrows(ResponseException.class, () -> {
                final Request request = new Request("POST", "/_test_blocking");
                request.addParameter("id", "1");
                request.addParameter("targets",
                    Stream.concat(newNodes.stream().map(n -> ":" + n), oldNodes.stream().map(n -> "old:" + n))
                        .collect(Collectors.joining(",")));
                newCluster.getLowLevelClient().performRequest(request);
            });
            try {
                final String reason = EntityUtils.toString(exception.getResponse().getEntity());
                assertThat(reason, containsString("\"type\":\"task_cancelled_exception\""));
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        });
        thread.start();
        try {
            assertBusy(() -> {
                List<TaskInfo> oldTasks = oldCluster.tasks()
                    .list(new ListTasksRequest().setActions("internal::test_action"), RequestOptions.DEFAULT)
                    .getTasks().stream().filter(t -> oldNodes.contains(t.getTaskId().getNodeId())).collect(Collectors.toList());
                assertThat(oldTasks.size(), greaterThanOrEqualTo(oldNodes.size()));
                List<TaskInfo> rootTasks = newCluster.tasks()
                    .list(new ListTasksRequest().setActions("internal::test_action"), RequestOptions.DEFAULT)
                    .getTasks().stream().filter(t -> t.getParentTaskId().isSet() == false)
                    .collect(Collectors.toList());
                assertThat(rootTasks, hasSize(1));

                // Now cancel tasks
                for (TaskInfo task : oldTasks) {
                    TaskId taskId = new TaskId(task.getTaskId().getNodeId(), task.getTaskId().getId());
                    CancelTasksRequest request = new CancelTasksRequest.Builder().withTaskId(taskId).withWaitForCompletion(false).build();
                    oldCluster.tasks().cancel(request, RequestOptions.DEFAULT);
                }
                for (TaskInfo task : rootTasks) {
                    TaskId taskId = new TaskId(task.getTaskId().getNodeId(), task.getTaskId().getId());
                    CancelTasksRequest request = new CancelTasksRequest.Builder().withTaskId(taskId).withWaitForCompletion(false).build();
                    newCluster.tasks().cancel(request, RequestOptions.DEFAULT);
                }
            });
            assertBusy(() -> {
                ListTasksRequest request = new ListTasksRequest().setActions("internal::test_action");
                assertThat(oldCluster.tasks().list(request, RequestOptions.DEFAULT).getTasks(), empty());
                assertThat(oldCluster.tasks().list(request, RequestOptions.DEFAULT).getTasks(), empty());
            });
        } finally {
            thread.join(120 * 1000, 0);
        }
    }
}
