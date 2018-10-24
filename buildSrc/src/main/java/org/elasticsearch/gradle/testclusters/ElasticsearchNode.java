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
import org.elasticsearch.gradle.Version;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ElasticsearchNode {

    private final String name;
    private final GradleServicesAdapter services;
    private final AtomicInteger noOfClaims = new AtomicInteger();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Logger logger = Logging.getLogger(ElasticsearchNode.class);

    private Distribution distribution;
    private Version version;

    public ElasticsearchNode(String name, GradleServicesAdapter services) {
        this.name = name;
        this.services = services;
    }

    public String getName() {
        return name;
    }

    public Version getVersion() {
        return version;
    }

    public void setVersion(Version version) {
        checkNotRunning();
        this.version = version;
    }

    public Distribution getDistribution() {
        return distribution;
    }

    public void setDistribution(Distribution distribution) {
        checkNotRunning();
        this.distribution = distribution;
    }

    public void claim() {
        noOfClaims.incrementAndGet();
        logger.debug("{} registered new claim", this);
    }

    /**
     * Start the cluster if not running. Does nothing if the cluster is already running.
     *
     */
    public void start() {
        if (started.getAndSet(true)) {
            logger.info("Already started `{}`", this);
            return;
        } else {
            logger.lifecycle("Starting `{}`", this);
        }
    }

    /**
     * Stops a running cluster if it's not claimed. Does nothing otherwise.
     */
    public void unClaimAndStop() {
        int decrementedClaims = noOfClaims.decrementAndGet();
        if (decrementedClaims > 0) {
            logger.lifecycle("Not stopping `{}`, since node still has {} claim(s)", this , decrementedClaims);
            return;
        }
        if (started.get() == false) {
            logger.lifecycle("`{}` was not running, no need to stop", this);
            return;
        }
        logger.lifecycle("Stopping `{}`, number of claims is {}", this, decrementedClaims);
    }

    void forceStop() {
        logger.lifecycle("Forcefully stopping `{}`, number of claims is {}", this, noOfClaims.get());
    }

    private void checkNotRunning() {
        if (started.get()) {
            throw new IllegalStateException("Configuration can not be altered while running ");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticsearchNode that = (ElasticsearchNode) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "ElasticsearchNode{" +
            "name='" + name + '\'' +
            ", noOfClaims=" + noOfClaims +
            ", started=" + started +
            '}';
    }
}
