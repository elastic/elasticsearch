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
package org.elasticsearch.gradle.clusterformation;

interface ElasticsearchConfigurationInternal extends ElasticsearchConfiguration {

    /**
     * Stop elasticsearch, regardless of claims
     *
     * Called when a task that uses the cluster fails.
     * Might be called even if {@link #start} was not called.
     */
    void forceStop();

    /**
     * Signals that a task will use this cluster.
     *
     * This is called once for every task in the task graph that is configured to
     * {@link ClusterFormationTaskExtension#use(ElasticsearchConfigurationInternal)} elasticsearch.
     */
    void claim();

    /**
     * Start elasticsearch
     *
     * Called before a task that wants to use it executes.
     * Might be called multiple times.
     */
    void start();

    /**
     * Signals that a task is done with the cluster and it might terminate.
     *
     * Might be called multiple times or when elasticsearch is not running.
     * It should only terminate if there are no more claims.
     * Might be called even if {@link #start} was not called.
     */
    void unClaimAndStop();

}
