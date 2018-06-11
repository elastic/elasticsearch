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
package org.elasticsearch.clusterformation;

import org.gradle.api.logging.Logger;

import javax.inject.Inject;
import java.io.File;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ElasticsearchCluster {

    private final String name;

    private File distribution;

    private final AtomicInteger noOfClaims = new AtomicInteger();
    private final AtomicBoolean started = new AtomicBoolean(false);

    private final Logger logger;

    public ElasticsearchCluster(String name, Logger logger) {
        this.name = name;
        this.logger = logger;
    }

    public String getName() {
        return name;
    }

    public File getDistribution() {
        return distribution;
    }

    public void setDistribution(File distribution) {
        this.distribution = distribution;
    }

    public void claim() {
        noOfClaims.incrementAndGet();
    }

    /**
     * Start the cluster if not running. Does nothing if the cluster is already running.
     * @return future of thread running in the background
     */
    public Future<Void> start() {
        if (started.getAndSet(true)) {
            logger.lifecycle("Already started cluster: {}", name);
        } else {
            logger.lifecycle("Starting cluster: {}", name);
        }
        return null;
    }

    /**
     * Stops a running cluster if it's not claimed. Does nothing otherwise.
     */
    public void unClaimAndStop() {
        int decrementedClaims = noOfClaims.decrementAndGet();
        if (decrementedClaims > 0) {
            logger.lifecycle("Asked to unClaimAndStop {}, since cluster still has {} claim it will not be stopped",
                name, decrementedClaims
            );
            return;
        }
        if (started.get() == false) {
            logger.lifecycle("Asked to unClaimAndStop, but cluster was not running: {}", name);
            return;
        }
        logger.lifecycle("Stopping {}, since no of claims is {}", name, decrementedClaims);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticsearchCluster that = (ElasticsearchCluster) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
