/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.jmx;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.Node;
import org.elasticsearch.jmx.action.GetJmxServiceUrlAction;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.settings.Settings;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.*;
import static org.elasticsearch.util.concurrent.DynamicExecutors.*;

/**
 * @author kimchy (Shay Banon)
 */
// TODO Disabled for now. Can be used to mbean proxy other nodes in the cluster from within the same console. Need the jmxruntime_optional jars though..,
public class JmxClusterService extends AbstractComponent {

    private final ClusterService clusterService;

    private final JmxService jmxService;

    private final GetJmxServiceUrlAction getJmxServiceUrlAction;

    private final ExecutorService clusterNodesJmxUpdater;

    public JmxClusterService(Settings settings, ClusterService clusterService, JmxService jmxService, final GetJmxServiceUrlAction getJmxServiceUrlAction) {
        super(settings);
        this.clusterService = clusterService;
        this.jmxService = jmxService;
        this.getJmxServiceUrlAction = getJmxServiceUrlAction;

        this.clusterNodesJmxUpdater = newSingleThreadExecutor(daemonThreadFactory(settings, "jmxService#updateTask"));

        if (jmxService.publishUrl() != null) {
            clusterService.add(new JmxClusterEventListener());
            for (final Node node : clusterService.state().nodes()) {
                clusterNodesJmxUpdater.execute(new Runnable() {
                    @Override public void run() {
                        String nodeServiceUrl = getJmxServiceUrlAction.obtainPublishUrl(node);
                        registerNode(node, nodeServiceUrl);
                    }
                });
            }
        }
    }

    public void close() {
        if (clusterNodesJmxUpdater != null) {
            clusterNodesJmxUpdater.shutdownNow();
        }
    }

    private void registerNode(Node node, String nodeServiceUrl) {
        try {
            JMXServiceURL jmxServiceURL = new JMXServiceURL(nodeServiceUrl);
            JMXConnector jmxConnector = JMXConnectorFactory.connect(jmxServiceURL, null);

            MBeanServerConnection connection = jmxConnector.getMBeanServerConnection();

//            for (ObjectName objectName : connection.queryNames(null, null)) {
//                try {
//                    MBeanProxy mBeanProxy = new MBeanProxy(remoteName, connection);
//                } catch (InstanceAlreadyExistsException e) {
//                    // ignore
//                } catch (Exception e) {
//                    logger.warn("Failed to register proxy mbean", e);
//                }
//            }
        } catch (Exception e) {
            logger.warn("Failed to register node [" + node + "] with serviceUrl [" + nodeServiceUrl + "]", e);
        }
    }

    private class JmxClusterEventListener implements ClusterStateListener {
        @Override public void clusterChanged(ClusterChangedEvent event) {
            if (!event.nodesChanged()) {
                return;
            }
            for (final Node node : event.nodesDelta().addedNodes()) {
                clusterNodesJmxUpdater.execute(new Runnable() {
                    @Override public void run() {
                        String nodeServiceUrl = getJmxServiceUrlAction.obtainPublishUrl(node);
                        registerNode(node, nodeServiceUrl);
                    }
                });
            }
        }
    }
}
