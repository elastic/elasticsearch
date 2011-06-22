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

package org.elasticsearch.zookeeper;


import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

/**
 * @author imotov
 */
public class ZooKeeperEnvironment {

    private final String rootNodePath;

    private final String clusterNodePath;

    private final String nodesNodePath;

    private final String globalSettingsNodePath;

    private final String clusterSettingsNodePath;

    private final String masterNodePath;

    @Inject public ZooKeeperEnvironment(Settings settings, ClusterName clusterName) {
        rootNodePath = settings.get("zookeeper.root", "/es");
        clusterNodePath = rootNodePath + "/" + settings.get("zookeeper.cluster", clusterName.value());
        nodesNodePath = clusterNodePath + "/" + "nodes";
        globalSettingsNodePath = rootNodePath + "/" + "settings";
        clusterSettingsNodePath = clusterNodePath + "/" + "settings";
        masterNodePath = clusterNodePath + "/" + "leader";
    }

    public String rootNodePath() {
        return rootNodePath;
    }

    public String clusterNodePath() {
        return clusterNodePath;
    }

    public String nodesNodePath() {
        return nodesNodePath;
    }

    public String masterNodePath() {
        return masterNodePath;
    }

    public String clusterSettingsNodePath() {
        return clusterSettingsNodePath;
    }

    public String globalSettingsNodePath() {
        return globalSettingsNodePath;
    }
}
