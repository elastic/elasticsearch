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
package org.elasticsearch.gradle.testclusters;

import org.elasticsearch.GradleServicesAdapter;
import org.elasticsearch.gradle.Distribution;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ElasticsearchCluster implements TestClusterConfiguration {

    private final String path;
    private final String clusterName;
    private final List<ElasticsearchNode> nodes = new ArrayList<>();
    private final File workingDirBase;

    public ElasticsearchCluster(String path, String clusterName, GradleServicesAdapter services, File artifactsExtractDir, File workingDirBase) {
        this.path = path;
        this.clusterName = clusterName;
        this.nodes.add(new ElasticsearchNode(path, clusterName + "-1", services, artifactsExtractDir, workingDirBase));
        this.workingDirBase = workingDirBase;
    }

    public void setNumberOfNodes(int numberOfNodes) {
        if (numberOfNodes < 1) {
            throw new IllegalArgumentException("Number of nodes should be >= 1 but was " + numberOfNodes);
        }
        if (numberOfNodes <= nodes.size()) {
            throw new IllegalArgumentException(
                "Trying to configure " + this + " to have " + numberOfNodes + " nodes but it already has " + getNumberOfNodes()
            );
        }
        ElasticsearchNode first = nodes.get(0);
        for (int i = nodes.size() + 1 ; i <= numberOfNodes; i++) {
            this.nodes.add(
                new ElasticsearchNode(
                    first.path, clusterName + "-" + i, first.services, first.artifactsExtractDir.toFile(), workingDirBase
                )
            );
        }
    }

    public int getNumberOfNodes() {
        return nodes.size();
    }

    public String getName() {
        return clusterName;
    }



    @Override
    public void setVersion(String version) {
        nodes.forEach(each -> each.setVersion(version));
    }

    @Override
    public void setDistribution(Distribution distribution) {
        nodes.forEach(each -> each.setDistribution(distribution));
    }

    @Override
    public void plugin(URI plugin) {
        nodes.forEach(each -> each.plugin(plugin));
    }

    @Override
    public void plugin(File plugin) {
        nodes.forEach(each -> each.plugin(plugin));
    }

    @Override
    public void keystore(String key, String value) {
        nodes.forEach(each -> each.keystore(key, value));
    }

    @Override
    public void keystore(String key, Supplier<CharSequence> valueSupplier) {
        nodes.forEach(each -> each.keystore(key, valueSupplier));
    }

    @Override
    public void setting(String key, String value) {
        nodes.forEach(each -> each.setting(key, value));
    }

    @Override
    public void setting(String key, Supplier<CharSequence> valueSupplier) {
        nodes.forEach(each -> each.setting(key, valueSupplier));
    }

    @Override
    public void systemProperty(String key, String value) {
        nodes.forEach(each -> each.systemProperty(key, value));
    }

    @Override
    public void systemProperty(String key, Supplier<CharSequence> valueSupplier) {
        nodes.forEach(each -> each.systemProperty(key, valueSupplier));
    }

    @Override
    public void environment(String key, String value) {
        nodes.forEach(each -> each.environment(key, value));
    }

    @Override
    public void environment(String key, Supplier<CharSequence> valueSupplier) {
        nodes.forEach(each -> each.environment(key, valueSupplier));
    }

    @Override
    public void freeze() {
        nodes.forEach(ElasticsearchNode::freeze);
    }

    @Override
    public void setJavaHome(File javaHome) {
        nodes.forEach(each -> each.setJavaHome(javaHome));
    }

    @Override
    public void start() {
        nodes.forEach(ElasticsearchNode::start);
    }

    @Override
    public String getHttpSocketURI() {
        return nodes.get(0).getHttpSocketURI();
    }

    @Override
    public String getTransportPortURI() {
        return nodes.get(0).getTransportPortURI();
    }

    @Override
    public List<String> getAllHttpSocketURI() {
        return nodes.stream().flatMap(each -> each.getAllHttpSocketURI().stream()).collect(Collectors.toList());
    }

    @Override
    public List<String> getAllTransportPortURI() {
        return nodes.stream().flatMap(each -> each.getAllTransportPortURI().stream()).collect(Collectors.toList());
    }

    @Override
    public void stop(boolean tailLogs) {
        nodes.forEach(each -> each.stop(tailLogs));
    }

    public void eachVersionedDistribution(BiConsumer<String, Distribution> consumer) {
        nodes.forEach(each -> consumer.accept(each.getVersion(), each.getDistribution()));
    }

    public ElasticsearchNode singleNode() {
        if (nodes.size() != 1) {
            throw new IllegalStateException("Can't threat cluster as single node as it has " + nodes.size() + " nodes");
        }
        return nodes.get(0);
    }
}
